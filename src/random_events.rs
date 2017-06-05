use random::sample_single;
use std::collections::BTreeMap;
use params::{Probability, SimulationParams};
use name::Name;
use node::Node;
use event::Event;
use random::{random, do_with_probability};
use simulation::Phase;

pub struct RandomEvents {
    params: SimulationParams,
}

impl RandomEvents {
    pub fn new(params: SimulationParams) -> Self {
        RandomEvents { params }
    }

    pub fn get_events(&self, phase: Phase, nodes: &BTreeMap<Name, Node>) -> Vec<Event> {
        let mut events = vec![];

        // Random join.
        if do_with_probability(self.params.prob_join(phase)) {
            events.push(self.random_add());
        }

        // Random remove.
        match self.params.prob_drop(phase) {
            Probability::PerNetwork(prob) => {
                if do_with_probability(prob) {
                    events.push(self.random_remove(nodes));
                }
            }
            Probability::PerNode(prob) => {
                for (node, _) in nodes {
                    if do_with_probability(prob) {
                        events.push(Event::RemoveNode(*node));
                    }
                }
            }
        }

        events
    }

    fn random_add(&self) -> Event {
        Event::AddNode(random())
    }

    fn random_remove(&self, nodes: &BTreeMap<Name, Node>) -> Event {
        Event::RemoveNode(Self::find_node_to_remove(nodes))
    }

    fn find_node_to_remove(nodes: &BTreeMap<Name, Node>) -> Name {
        sample_single(nodes.iter())
            .map(|(name, _)| *name)
            .unwrap()
    }
}
