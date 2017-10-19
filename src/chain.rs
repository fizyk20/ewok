use std::ops::Deref;
use std::collections::BTreeMap;
use std::cmp;

use block::{BlockId, Block, NetworkEvent};
use name::Name;

#[derive(Clone, Debug, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Chain {
    blocks: Vec<Block>,
    index: BTreeMap<BlockId, usize>,
}

impl Deref for Chain {
    type Target = Vec<Block>;

    fn deref(&self) -> &Self::Target {
        &self.blocks
    }
}

impl Chain {
    pub fn new() -> Chain {
        Chain {
            blocks: Vec::new(),
            index: BTreeMap::new(),
        }
    }

    pub fn append(&mut self, block: Block) -> BlockId {
        let id = block.get_id();
        self.blocks.push(block);
        self.index.insert(id, self.blocks.len() - 1);
        id
    }

    pub fn get(&self, id: &BlockId) -> Option<&Block> {
        self.index.get(id).and_then(|index| self.blocks.get(*index))
    }

    /// Returns the index of the last valid block in the chain
    pub fn last_valid(&self) -> Option<usize> {
        self.blocks
            .iter()
            .enumerate()
            .rev()
            .find(|&(_, ref b)| b.has_consensus())
            .map(|(i, _)| i)
    }

    /// Returns the index of the last valid block in the chain
    pub fn last_valid_block(&self) -> Option<&Block> {
        self.blocks.iter().rev().find(|b| b.has_consensus())
    }

    /// Swaps the blocks with given indices
    /// Returns the index of the last valid block after the operation
    fn swap_blocks(&mut self, index1: usize, index2: usize) -> Option<usize> {
        let id1 = self.blocks[index1].get_id();
        let id2 = self.blocks[index2].get_id();
        // save new indices
        let _ = self.index.insert(id1, index2);
        let _ = self.index.insert(id2, index1);
        // swap the blocks
        self.blocks.swap(index1, index2);

        // recalculate member lists in affected blocks
        let mut min_index = cmp::min(index1, index2);
        if min_index == 0 {
            self.blocks[0].adjust_votes(BTreeMap::new());
            min_index = 1;
        }
        let mut last_valid = None;
        let n = self.blocks.len();
        for i in min_index..n {
            let votes = self.blocks[i - 1].apply_event();
            self.blocks[i].adjust_votes(votes);
            if self.blocks[i].has_consensus() {
                last_valid = Some(i);
            }
        }
        last_valid
    }

    /// Adds votes to the block with a given id. If the block wasn't valid, moves it
    /// to the position right after the last valid block.
    /// Returns whether the block became valid
    pub fn add_vote(&mut self, id: BlockId, voter: Name) -> bool {
        // Check if we already have the block
        if let Some(index) = self.index.get(&id).cloned() {
            // we do - check if valid
            if self.blocks[index].has_consensus() {
                self.blocks[index].add_vote(voter);
                false
            } else {
                // find index to swap
                let swap_index = self.last_valid()      // get last valid block index
                    .map(|i| i+1)
                    .unwrap_or(0); // if there was no valid block, we swap with 0
                // put the block in the correct position and add new votes
                self.swap_blocks(index, swap_index);
                self.blocks[swap_index].add_vote(voter);
                self.blocks[swap_index].has_consensus()
            }
        } else {
            let new_block = if self.blocks.is_empty() {
                Block::genesis(id.into_event())
            } else {
                self.blocks[self.blocks.len() - 1].from_event(id.into_event())
            };
            self.append(new_block);
            let new_index = self.blocks.len() - 1;
            let swap_index = self.last_valid()      // get last valid block index
                    .map(|i| i+1)
                    .unwrap_or(0); // if there was no valid block, we swap with 0
            // put the block in the correct position and add new votes
            self.swap_blocks(new_index, swap_index);
            self.blocks[swap_index].add_vote(voter);
            self.blocks[swap_index].has_consensus()
        }
    }

    /// Creates a block with a node added, if not yet present; returns the block id if added
    pub fn add_node(&mut self, node: Name) -> Option<BlockId> {
        let block_id = BlockId(NetworkEvent::NodeGained(node));
        if self.index.get(&block_id).is_none() {
            let new_block = if let Some(block) = self.blocks.last() {
                block.from_event(NetworkEvent::NodeGained(node))
            } else {
                Block::genesis(NetworkEvent::NodeGained(node))
            };
            self.append(new_block);
            Some(block_id)
        } else {
            None
        }
    }

    /// Creates a block with a node added, if not yet present; returns the block id if added
    pub fn remove_node(&mut self, node: Name) -> Option<BlockId> {
        let block_id = BlockId(NetworkEvent::NodeLost(node));
        if self.index.get(&block_id).is_none() {
            // borrow checker hack
            let block = if let Some(block) = self.blocks.last() {
                block.clone()
            } else {
                // WTF happened?
                // we try to remove a node from an empty chain
                return None;
            };
            let new_block = block.from_event(NetworkEvent::NodeLost(node));
            self.append(new_block);
            Some(block_id)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn short_name(name: u8) -> Name {
        Name((name as u64) << (64 - 8))
    }

    #[test]
    fn covering() {
        let mut blocks = Chain::new();
        let block1 = blocks.insert(Block {
            prefix: Prefix::empty(),
            version: 0,
            members: btreeset!{ Name(0), short_name(0b10000000) },
        });
        let block2 = blocks.insert(Block {
            prefix: Prefix::short(1, 0),
            version: 1,
            members: btreeset!{ Name(0) },
        });
        let valid_blocks = btreeset![block1, block2];

        let expected_current = btreeset![block1];

        let candidates = blocks.compute_current_candidate_blocks(valid_blocks);
        let current_blocks = blocks.compute_current_blocks(&candidates);

        assert_eq!(expected_current, current_blocks);
    }

    #[test]
    fn segment() {
        let b1_members =
            btreeset!{ Name(0), Name(1), Name(2), Name(3 & (1 << 63)), Name(4 & (1 << 63)) };
        let b1 = Block {
            prefix: Prefix::empty(),
            version: 0,
            members: b1_members.clone(),
        };
        let b2_members = btreeset!{ Name(0), Name(1), Name(2) };
        let b2 = Block {
            prefix: Prefix::short(1, 0),
            version: 1,
            members: b2_members.clone(),
        };
        let b3_members = &b2_members | &btreeset!{ Name(5) };
        let b3 = Block {
            prefix: Prefix::short(1, 0),
            version: 2,
            members: b3_members.clone(),
        };
        let mut blocks = Chain::new();
        let b1_id = blocks.insert(b1);
        let b2_id = blocks.insert(b2);
        let b3_id = blocks.insert(b3);

        let rev_votes =
            btreemap! {
            b2_id => btreemap! {
                b1_id => b1_members.clone(),
            },
            b3_id => btreemap! {
                b2_id => b2_members.clone(),
            },
        };

        let segment_votes = blocks.chain_segment(&b3_id, &rev_votes);

        let v12 = Vote {
            from: b1_id,
            to: b2_id,
        };
        let v23 = Vote {
            from: b2_id,
            to: b3_id,
        };
        let expected =
            btreeset! {
            (v12, b1_members),
            (v23, b2_members),
        };
        assert_eq!(segment_votes, expected);
    }
}
