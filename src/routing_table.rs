use chain::Chain;
use block::BlockId;
use std::collections::{BTreeSet, BTreeMap};
use name::{Name, Prefix};

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct RoutingTable {
    our_prefix: Prefix,
    chains: BTreeMap<Prefix, Chain>,
}

impl RoutingTable {
    pub fn new() -> RoutingTable {
        let mut chains = BTreeMap::new();
        let null_prefix = Prefix::empty();
        chains.insert(null_prefix, Chain::new());
        RoutingTable {
            our_prefix: null_prefix,
            chains,
        }
    }

    pub fn from_chains(name: Name, chains: &BTreeMap<Prefix, Chain>) -> RoutingTable {
        let our_prefix = *chains.keys().find(|prefix| prefix.matches(name)).unwrap();
        RoutingTable {
            our_prefix,
            chains: chains
                .into_iter()
                .filter(|&(prefix, _)| prefix.is_neighbour(&our_prefix))
                .map(|(prefix, chain)| (*prefix, chain.clone()))
                .collect(),
        }
    }

    pub fn current_nodes(&self) -> BTreeSet<Name> {
        self.chains
            .iter()
            .filter_map(|(_, chain)| chain.last_valid_block())
            .flat_map(|b| b.members.keys().cloned())
            .collect()
    }

    pub fn our_prefix(&self) -> Prefix {
        self.our_prefix
    }

    pub fn our_section(&self) -> Option<BTreeSet<Name>> {
        self.chains
            .get(&self.our_prefix)
            .and_then(|chain| chain.last_valid_block())
            .map(|block| block.members.keys().cloned().collect())
    }

    fn get_section(&self, name: Name) -> Option<Prefix> {
        for prefix in self.chains.keys() {
            if prefix.matches(name) {
                return Some(*prefix);
            }
        }
        None
    }

    pub fn add_votes<I>(&mut self, block: BlockId, voters: I) -> bool
    where
        I: IntoIterator<Item = Name>,
    {
        let mut result = false;
        for voter in voters {
            if let Some(prefix) = self.get_section(voter) {
                self.chains.get_mut(&prefix).and_then(|chain| {
                    result = result || chain.add_vote(block, voter);
                    Some(())
                });
            }
        }
        result
    }

    pub fn add_node(&mut self, node: Name) -> Option<BlockId> {
        let section = self.get_section(node);
        section
            .and_then(|prefix| self.chains.get_mut(&prefix))
            .and_then(|chain| chain.add_node(node))
    }

    pub fn remove_node(&mut self, node: Name) -> Option<BlockId> {
        let section = self.get_section(node);
        section
            .and_then(|prefix| self.chains.get_mut(&prefix))
            .and_then(|chain| chain.remove_node(node))
    }

    pub fn merge(&mut self, other: RoutingTable) {
        // TODO
        *self = other;
    }
}
