//! Functions for generating sections of a certain size.

use block::{NetworkEvent, Block};
use chain::Chain;
use routing_table::RoutingTable;
use name::{Name, Prefix};
use node::Node;
use params::NodeParams;
use random::random;

use std::collections::{BTreeMap, BTreeSet};

/// Generate a bunch of nodes based on sizes specified for sections.
///
/// `sections`: map from prefix to desired size for that section.
pub fn generate_network(
    sections: &BTreeMap<Prefix, usize>,
    params: &NodeParams,
) -> (BTreeMap<Name, Node>, RoutingTable) {
    // Check that the supplied prefixes describe a whole network.
    assert!(
        Prefix::empty().is_covered_by(sections.keys()),
        "Prefixes should cover the whole namespace"
    );

    let mut nodes_by_section = btreemap!{};

    for (prefix, &size) in sections {
        let node_names: BTreeSet<_> = (0..size).map(|_| prefix.substituted_in(random())).collect();
        nodes_by_section.insert(*prefix, node_names);
    }

    let current_chains = construct_chains(nodes_by_section.clone());

    let nodes = nodes_by_section
        .into_iter()
        .flat_map(|(_, names)| names)
        .map(|name| {
            let routing_table = RoutingTable::from_chains(name, &current_chains);
            (name, Node::new(name, routing_table, params.clone(), 0))
        })
        .collect();

    (nodes, RoutingTable::new())
}

/// Construct a set of blocks to describe the given sections.
fn construct_chains(nodes: BTreeMap<Prefix, BTreeSet<Name>>) -> BTreeMap<Prefix, Chain> {
    // Hack - use a split block as the initial block in chains for all prefixes
    // TODO: find a better way
    nodes
        .into_iter()
        .map(|(prefix, members)| {
            let block = Block {
                event: NetworkEvent::Split(prefix),
                members: members.into_iter().map(|name| (name, true)).collect(),
                invalid_votes: BTreeSet::new(),
            };
            let mut chain = Chain::new();
            chain.append(block);
            (prefix, chain)
        })
        .collect()
}
