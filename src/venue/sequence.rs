use crate::types::Sequence;

#[derive(Debug, Default)]
pub struct SequenceTracker {
    last_sequence: Option<Sequence>,
}

#[derive(Debug)]
pub enum SequenceStatus {
    First,
    Newer,
    DuplicateOrOld,
}

#[derive(Debug, Clone, Copy)]
pub enum DepthSequenceStatus {
    Applied,
    IgnoredOldUpdate,
    Gap,
}

impl SequenceTracker {
    pub fn check_book_ticker(&mut self, received: Sequence) -> SequenceStatus {
        match self.last_sequence {
            None => {
                self.last_sequence = Some(received);
                SequenceStatus::First
            }
            Some(last) if received <= last => SequenceStatus::DuplicateOrOld,
            Some(_) => {
                self.last_sequence = Some(received);
                SequenceStatus::Newer
            }
        }
    }
}