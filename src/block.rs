use name::{Prefix, Name};
use std::mem;

use std::collections::{BTreeSet, BTreeMap};

#[derive(Clone, Debug, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockId(pub NetworkEvent);

impl BlockId {
    pub fn into_event(self) -> NetworkEvent {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum NetworkEvent {
    NodeGained(Name),
    NodeLost(Name),
    Merge(Prefix),
    Split(Prefix),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Block {
    pub event: NetworkEvent,
    pub members: BTreeMap<Name, bool>,
    pub invalid_votes: BTreeSet<Name>,
}

impl Block {
    pub fn get_id(&self) -> BlockId {
        BlockId(self.event)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Vote {
    pub block_id: BlockId,
}

#[cfg(feature = "fast")]
fn abs_diff(x: usize, y: usize) -> usize {
    if x >= y { x - y } else { y - x }
}

impl Block {
    pub fn genesis(event: NetworkEvent) -> Block {
        Block {
            event,
            members: BTreeMap::new(),
            invalid_votes: BTreeSet::new(),
        }
    }

    pub fn clone_members(&self) -> BTreeMap<Name, bool> {
        self.members
            .iter()
            .map(|(&name, _)| (name, false))
            .collect()
    }

    pub fn apply_event(&self) -> BTreeMap<Name, bool> {
        let mut members = self.clone_members();
        match self.event {
            NetworkEvent::NodeGained(name) => {
                members.insert(name, false);
            }
            NetworkEvent::NodeLost(name) => {
                assert!(members.remove(&name).is_some());
            }
            NetworkEvent::Split(prefix) => {
                members = members
                    .into_iter()
                    .filter(|&(name, _)| prefix.matches(name))
                    .collect();
            }
            _ => (),
        }
        members
    }

    /// Sorts the collected votes into valid and invalid based on the new members list
    pub fn adjust_votes(&mut self, other: BTreeMap<Name, bool>) {
        // check who of the new members already voted
        let new_members = other
            .into_iter()
            .map(|(name, _)| {
                (
                    name,
                    self.members.get(&name).cloned().unwrap_or(false) ||
                        self.invalid_votes.contains(&name),
                )
            })
            .collect();
        let old_members = mem::replace(&mut self.members, new_members);
        // save old names that voted, even if invalid now
        let new_invalid_votes = old_members
            .into_iter()
            .filter(|&(name, voted)| voted && !self.members.contains_key(&name))
            .map(|(name, _)| name)
            .chain(
                self.invalid_votes
                    .iter()
                    .filter(|&name| !self.members.contains_key(name))
                    .cloned(),
            )
            .collect();
        self.invalid_votes = new_invalid_votes;
    }

    pub fn add_vote(&mut self, voter: Name) {
        if self.members.contains_key(&voter) {
            let _ = self.members.insert(voter, true);
        } else {
            self.invalid_votes.insert(voter);
        }
    }

    pub fn from_event(&self, event: NetworkEvent) -> Self {
        let members = self.apply_event();
        Block {
            event,
            members,
            invalid_votes: BTreeSet::new(),
        }
    }

    /// Create a new block with a node added.
    pub fn node_added(&self, added: Name) -> Self {
        let members = self.apply_event();
        Block {
            event: NetworkEvent::NodeGained(added),
            members,
            invalid_votes: BTreeSet::new(),
        }
    }

    /// Create a new block with a node removed.
    pub fn node_removed(&self, removed: Name) -> Self {
        let members = self.apply_event();
        Block {
            event: NetworkEvent::NodeLost(removed),
            members,
            invalid_votes: BTreeSet::new(),
        }
    }

    pub fn section_split(&self, to_prefix: Prefix) -> Self {
        let members = self.apply_event();
        Block {
            event: NetworkEvent::Split(to_prefix),
            members,
            invalid_votes: BTreeSet::new(),
        }
    }

    pub fn section_merge(&self, to_prefix: Prefix) -> Self {
        let members = self.apply_event();
        Block {
            event: NetworkEvent::Merge(to_prefix),
            members,
            invalid_votes: BTreeSet::new(),
        }
    }

    pub fn has_consensus(&self) -> bool {
        let num_votes = self.members.iter().filter(|&(_, voted)| *voted).count();
        num_votes * 2 > self.members.len()
    }
}
