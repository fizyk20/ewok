use block::{BlockId, Vote};
use routing_table::RoutingTable;
use name::{Name, Prefix};
use self::MessageContent::*;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Message {
    pub sender: Name,
    pub recipient: Name,
    pub content: MessageContent,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MessageContent {
    /// Vote for a block to succeed another block.
    VoteMsg(Vote),
    /// Notification that we believe this vote to be agreed by all the listed members.
    VoteAgreedMsg((Vote, BTreeSet<Name>)),
    /// Message sent from joining node (sender) to all section members (recipients).
    NodeJoined,
    /// Message sent to a joining node to get it up to date on the current blocks.
    BootstrapMsg(RoutingTable),
    /// Connect and disconnect represent the connection or disconnection of two nodes.
    /// Can be sent from node-to-node or from the simulation to a pair of nodes (for disconnects
    /// and reconnects).
    /// See handling in node.rs.
    Connect,
    /// ^See above.
    Disconnect,
}

// XOR distance between the lower bounds of two prefixes.
fn prefix_dist(p1: &Prefix, p2: &Prefix) -> u64 {
    p1.lower_bound().0 ^ p2.lower_bound().0
}

impl MessageContent {
    pub fn recipients(&self, our_name: Name, rt: &RoutingTable) -> BTreeSet<Name> {
        match *self {
            // Send votes to members of the `from` and `to` blocks.
            VoteMsg(_) => rt.our_section().unwrap_or_else(BTreeSet::new),
            // Send anything else to all connected neighbours.
            _ => rt.current_nodes(),
        }
    }
}
