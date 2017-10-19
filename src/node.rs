use message::Message;
use message::MessageContent;
use message::MessageContent::*;
use name::Name;
use block::Vote;
use routing_table::RoutingTable;
use params::NodeParams;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt;

const MESSAGE_FILTER_LEN: usize = 1024;

#[derive(Debug, Clone)]
pub struct Node {
    /// Our node's name.
    pub our_name: Name,
    /// The block storage
    pub routing_table: RoutingTable,
    /// Peers that we're currently connected to.
    pub connections: BTreeSet<Name>,
    /// Nodes that we've sent connection requests to.
    pub connect_requests: BTreeSet<Name>,
    /// Candidates who we are waiting to add to our current blocks.
    pub candidates: BTreeMap<Name, Candidate>,
    /// Filter for hashes of recent messages we've already sent and shouldn't resend.
    pub message_filter: VecDeque<u64>,
    /// Network configuration parameters.
    pub params: NodeParams,
    /// Step that this node was created.
    pub step_created: u64,
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Node({})", self.our_name)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Candidate {
    step_added: u64,
}

impl Candidate {
    fn is_recent(&self, join_timeout: u64, step: u64) -> bool {
        self.step_added + join_timeout >= step
    }
}

impl Node {
    /// Create a new node which starts from a given routing table
    pub fn new(name: Name, rt: RoutingTable, params: NodeParams, step: u64) -> Self {
        // FIXME: prune connections
        let connections = rt.current_nodes();

        Node {
            our_name: name,
            routing_table: rt,
            connections,
            connect_requests: BTreeSet::new(),
            candidates: BTreeMap::new(),
            message_filter: VecDeque::with_capacity(MESSAGE_FILTER_LEN),
            params,
            step_created: step,
        }
    }

    /// Minimum size that all sections must be before splitting.
    fn min_split_size(&self) -> usize {
        self.params.min_section_size + self.params.split_buffer
    }

    /// Insert a vote into our local cache of votes.
    /// Returns whether the vote triggered a block becoming valid
    fn add_vote<I>(&mut self, vote: Vote, voted_for: I) -> bool
    where
        I: IntoIterator<Item = Name>,
    {
        self.routing_table.add_votes(vote.block_id, voted_for)
    }

    fn is_candidate(&self, name: &Name, step: u64) -> bool {
        self.candidates
            .get(name)
            .map(|candidate| {
                candidate.is_recent(self.params.join_timeout, step) &&
                    self.connections.contains(name)
            })
            .unwrap_or(false)
    }

    /// Get connection and disconnection messages for peers.
    fn connects_and_disconnects(&mut self, step: u64) -> Vec<Message> {
        let neighbours = self.routing_table.current_nodes();
        let our_name = self.our_name;

        // FIXME: put this somewhere else?
        for node in &neighbours {
            self.candidates.remove(node);
        }

        let to_disconnect: BTreeSet<Name> = {
            self.connections
                .iter()
                .filter(|name| {
                    !neighbours.contains(&name) && !self.is_candidate(&name, step)
                })
                .cloned()
                .collect()
        };

        for node in &to_disconnect {
            trace!("{}: disconnecting from {}", self, node);
            self.connections.remove(node);
            self.connect_requests.remove(node);
        }

        let disconnects = to_disconnect.into_iter().map(|neighbour| {
            Message {
                sender: our_name,
                recipient: neighbour,
                content: MessageContent::Disconnect,
            }
        });

        let to_connect: BTreeSet<Name> = {
            neighbours
                .iter()
                .filter(|name| {
                    !self.connections.contains(name) && !self.connect_requests.contains(name) &&
                        **name != our_name
                })
                .cloned()
                .collect()
        };

        for node in &to_connect {
            trace!("{}: connecting to {}", self, node);
            self.connect_requests.insert(*node);
        }

        let connects = to_connect.into_iter().map(|neighbour| {
            Message {
                sender: our_name,
                recipient: neighbour,
                content: MessageContent::Connect,
            }
        });

        connects.chain(disconnects).collect()
    }

    pub fn our_current_section(&self) -> Option<BTreeSet<Name>> {
        self.routing_table.our_section()
    }

    /// Called once per step.
    pub fn update_state(&mut self, step: u64) -> Vec<Message> {
        let mut messages = vec![];

        // Generate connect and disconnect messages.
        messages.extend(self.connects_and_disconnects(step));

        messages
    }

    /// Create messages for every relevant neighbour for every vote in the given vec.
    pub fn broadcast(&self, msgs: Vec<MessageContent>, step: u64) -> Vec<Message> {
        msgs.into_iter()
            .flat_map(move |content| {
                let mut recipients = content.recipients(self.our_name, &self.routing_table);
                recipients.extend(self.nodes_to_add(step));
                recipients.remove(&self.our_name);

                recipients.into_iter().map(move |recipient| {
                    Message {
                        sender: self.our_name,
                        recipient,
                        content: content.clone(),
                    }
                })
            })
            .collect()
    }

    fn nodes_to_add(&self, step: u64) -> Vec<Name> {
        self.candidates
            .iter()
            .filter(|&(name, candidate)| {
                self.connections.contains(name) &&
                    candidate.is_recent(self.params.join_timeout, step)
            })
            .map(|(name, _)| *name)
            .collect()
    }

    fn nodes_to_drop(&self) -> Vec<Name> {
        self.routing_table
            .current_nodes()
            .into_iter()
            .filter(|peer| {
                *peer != self.our_name && !self.connections.contains(peer) &&
                    !self.candidates.contains_key(peer)
            })
            .collect()
    }

    /// Construct new successor blocks based on our view of the network.
    pub fn construct_new_votes(&mut self, step: u64) -> Vec<Vote> {
        let mut votes = vec![];

        for node in self.nodes_to_add(step) {
            if let Some(block) = self.routing_table.add_node(node) {
                votes.push(Vote { block_id: block });
            }
        }

        for node in self.nodes_to_drop() {
            if let Some(block) = self.routing_table.remove_node(node) {
                votes.push(Vote { block_id: block });
            }
        }

        votes
    }

    /// Returns new votes to be broadcast after filtering them.
    pub fn broadcast_new_votes(&mut self, step: u64) -> Vec<Message> {
        let votes = self.construct_new_votes(step);
        let our_name = self.our_name;

        let mut to_broadcast = vec![];

        for vote in &votes {
            self.add_vote(vote.clone(), Some(our_name));
        }

        // Construct vote messages and broadcast.
        let vote_msgs: Vec<_> = votes.into_iter().map(VoteMsg).collect();
        to_broadcast.extend(self.broadcast(vote_msgs, step));

        self.filter_messages(to_broadcast)
    }

    /// Remove messages that have already been sent from `messages`, and update the filter.
    fn filter_messages(&mut self, messages: Vec<Message>) -> Vec<Message> {
        let mut filtered = vec![];
        for message in messages {
            let mut hasher = DefaultHasher::new();
            message.hash(&mut hasher);
            let hash = hasher.finish();
            if message.content == Connect || message.content == Disconnect ||
                !self.message_filter.contains(&hash)
            {
                filtered.push(message);
                if self.message_filter.len() == MESSAGE_FILTER_LEN {
                    let _ = self.message_filter.pop_front();
                }
                self.message_filter.push_back(hash);
            }
        }
        filtered
    }

    /// Create a message with all our votes to send to a new node.
    fn construct_bootstrap_msg(&self, joining_node: Name) -> Message {
        Message {
            sender: self.our_name,
            recipient: joining_node,
            content: BootstrapMsg(self.routing_table.clone()),
        }
    }

    /// Apply a bootstrap message received from another node.
    fn apply_bootstrap_msg(&mut self, routing_table: RoutingTable) {
        self.routing_table.merge(routing_table);
    }

    /// Returns true if the peer is known and its state is `Disconnected`.
    pub fn is_disconnected_from(&self, name: &Name) -> bool {
        !self.connections.contains(name)
    }

    /// Returns true if this node should shutdown because it has failed to join a section.
    pub fn should_shutdown(&self, step: u64) -> bool {
        let timeout_elapsed = step >= self.step_created + self.params.self_shutdown_timeout;

        let (no_blocks, insufficient_connections) =
            if let Some(section) = self.routing_table.our_section() {
                (false, self.connections.len() * 2 < section.len())
            } else {
                (true, true)
            };

        timeout_elapsed && (no_blocks || insufficient_connections)
    }

    pub fn step_created(&self) -> u64 {
        self.step_created
    }

    fn should_be_connected(&self, node: Name) -> bool {
        let neighbours = self.routing_table.current_nodes();
        neighbours.contains(&node)
    }

    /// Handle a message intended for us and return messages we'd like to send.
    pub fn handle_message(&mut self, message: Message, step: u64) -> Vec<Message> {
        let to_send = match message.content {
            NodeJoined => {
                let joining_node = message.sender;
                debug!("{}: received join message for: {}", self, joining_node);

                // Mark the peer as having joined so that we vote to keep adding it.
                self.candidates.insert(
                    joining_node,
                    Candidate { step_added: step },
                );
                self.connections.insert(joining_node);
                self.connect_requests.insert(joining_node);

                let connect_msg = Message {
                    sender: self.our_name,
                    recipient: joining_node,
                    content: Connect,
                };

                // Send a bootstrap message to the joining node.
                vec![connect_msg, self.construct_bootstrap_msg(joining_node)]
            }
            VoteMsg(vote) => {
                trace!("{}: received {:?} from {}", self, vote, message.sender);
                self.add_vote(vote, Some(message.sender));
                vec![]
            }
            VoteAgreedMsg((vote, voters)) => {
                trace!(
                    "{}: received agreement msg for {:?} from {}",
                    self,
                    vote,
                    message.sender
                );
                self.add_vote(vote, voters);
                vec![]
            }
            BootstrapMsg(routing_table) => {
                debug!(
                    "{}: applying bootstrap message from {}",
                    self,
                    message.sender
                );
                self.apply_bootstrap_msg(routing_table);
                vec![]
            }
            Disconnect => {
                debug!("{}: lost our connection to {}", self, message.sender);
                self.connections.remove(&message.sender);
                self.connect_requests.remove(&message.sender);
                vec![]
            }
            Connect => {
                if self.should_be_connected(message.sender) {
                    if self.connections.insert(message.sender) {
                        debug!("{}: obtained a connection to {}", self, message.sender);
                    }
                    if !self.connect_requests.contains(&message.sender) {
                        trace!("{}: connecting back to {}", self, message.sender);
                        self.connect_requests.insert(message.sender);
                        vec![
                            Message {
                                sender: self.our_name,
                                recipient: message.sender,
                                content: MessageContent::Connect,
                            },
                        ]
                    } else {
                        vec![]
                    }
                } else {
                    trace!(
                        "{}: rejecting connection request from {}",
                        self,
                        message.sender
                    );
                    self.connections.remove(&message.sender);
                    self.connect_requests.remove(&message.sender);
                    vec![
                        Message {
                            sender: self.our_name,
                            recipient: message.sender,
                            content: Disconnect,
                        },
                    ]
                }
            }
        };

        self.filter_messages(to_send)
    }
}
