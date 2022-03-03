use crate::data::SegmentID;

use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

pub(super) type SequenceID = u64;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub(super) enum QueueFile {
    WriteAheadLog(SegmentID),
    Segment(SegmentKey),
    TemporarySegmentDirectory(SegmentID),
    DeletionMarker,
}

impl QueueFile {
    pub(super) fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
        let name = path.as_ref().file_name()?.to_str()?;
        if name == "deleted" {
            return Some(Self::DeletionMarker);
        }

        let (name, ext) = name.rsplit_once('.')?;
        match ext {
            "pending" | "tombstone " => Some(Self::Segment(SegmentKey::from_path(path)?)),
            "log" => {
                let segment_id = SegmentID::from_str_radix(name, 16).ok()?;
                Some(Self::WriteAheadLog(segment_id))
            }
            "segment" => {
                let segment_id = SegmentID::from_str_radix(name, 16).ok()?;
                Some(Self::TemporarySegmentDirectory(segment_id))
            }
            _ => None,
        }
    }

    pub(super) fn to_path<P: AsRef<Path>>(self, directory: P) -> PathBuf {
        let name = match self {
            Self::WriteAheadLog(id) => format!("{:016x}.log", id),
            Self::TemporarySegmentDirectory(id) => format!("{:016x}.segment", id),
            Self::Segment(key) => key.to_string(),
            Self::DeletionMarker => "deleted".to_string(),
        };

        directory.as_ref().join(name)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub(super) enum SegmentKey {
    Pending {
        start: SegmentID,
        end: SegmentID,
        seq_id: SequenceID,
    },
    Tombstone {
        start: SegmentID,
        end: SegmentID,
        seq_id: SequenceID,
    },
}

impl SegmentKey {
    pub(super) fn pending(start: SegmentID, end: SegmentID, seq_id: SequenceID) -> Self {
        Self::Pending { start, end, seq_id }
    }

    pub(super) fn tombstone(start: SegmentID, end: SegmentID, seq_id: SequenceID) -> Self {
        Self::Tombstone { start, end, seq_id }
    }

    pub(super) fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    pub(super) fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone { .. })
    }

    pub(super) fn unpack(self) -> (SegmentID, SegmentID, SequenceID) {
        match self {
            Self::Pending { start, end, seq_id } => (start, end, seq_id),
            Self::Tombstone { start, end, seq_id } => (start, end, seq_id),
        }
    }

    pub(super) fn start(&self) -> SegmentID {
        self.unpack().0
    }

    pub(super) fn end(&self) -> SegmentID {
        self.unpack().1
    }

    pub(super) fn seq_id(&self) -> SequenceID {
        self.unpack().2
    }

    pub(super) fn range(self) -> RangeInclusive<SegmentID> {
        let (start, end, _) = self.unpack();
        start..=end
    }

    fn kind_str(&self) -> &'static str {
        match self {
            Self::Pending { .. } => "pending",
            Self::Tombstone { .. } => "tombstone",
        }
    }

    fn from_str(s: &str) -> Option<SegmentKey> {
        let (rest, kind) = s.rsplit_once('.')?;
        let (range, seq_id) = rest.split_once('.')?;
        let (start, end) = range.split_once('-')?;

        let start = SegmentID::from_str_radix(start, 16).ok()?;
        let end = SegmentID::from_str_radix(end, 16).ok()?;
        let seq_id = SequenceID::from_str_radix(seq_id, 16).ok()?;

        match kind {
            "pending" => Some(Self::Pending { start, end, seq_id }),
            "tombstone" => Some(Self::Tombstone { start, end, seq_id }),
            _ => None,
        }
    }

    pub(super) fn to_path<P: AsRef<Path>>(self, directory: P) -> PathBuf {
        directory.as_ref().join(self.to_string())
    }

    pub(super) fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
        path.as_ref()
            .file_name()
            .and_then(|s| s.to_str())
            .and_then(Self::from_str)
    }
}

impl Display for SegmentKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (start, end, seq_id) = self.unpack();
        let kind = self.kind_str();
        write!(f, "{:016x}-{:016x}.{:016x}.{}", start, end, seq_id, kind)
    }
}

#[cfg(test)]
mod tests {
    use super::SegmentKey;

    #[test]
    pub fn test() {
        let s = "0000000000000000-0000000000000000.0000000000000000.pending";
        assert_eq!(Some(SegmentKey::pending(0, 0, 0)), SegmentKey::from_str(s));
    }
}
