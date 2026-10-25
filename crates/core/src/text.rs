//! The line-based text merge: disjoint changes merge cleanly, and
//! overlapping ones come back between git-style `<<<<<<<` markers.
//!
//! Byte for byte the merge Python kvgit has always made: lines split as
//! `str.splitlines(keepends=True)` splits them, each side diffed against
//! the base by difflib's `SequenceMatcher` (no junk heuristic), and
//! changes grouped where they overlap.

use std::collections::HashMap;
use std::fmt;

/// Inputs whose combined size exceeds this are refused rather than
/// merged. Line diffing is quadratic at worst (a minified line of
/// megabytes), and a merge this big is a whole-file conflict anyway.
pub const MAX_MARK_BYTES: usize = 1024 * 1024;

/// Why the text merge made nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextError {
    /// The values can't be marker-merged: not UTF-8, a NUL byte, over
    /// [`MAX_MARK_BYTES`], or a conflict under a strict merge. A merge
    /// files the key as a conflict.
    CantMark(String),
    /// A label holds a line break, which would split its marker line.
    /// The caller's mistake, not the values'.
    BadLabel { name: &'static str, label: String },
}

impl fmt::Display for TextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TextError::CantMark(message) => f.write_str(message),
            TextError::BadLabel { name, label } => {
                write!(f, "{name} may not contain a newline: {label:?}")
            }
        }
    }
}

impl std::error::Error for TextError {}

/// The labels and strictness of one text merge rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextRule {
    pub ours_label: String,
    pub theirs_label: String,
    /// Refuse, as [`TextError::CantMark`], where markers would be
    /// written.
    pub strict: bool,
}

impl Default for TextRule {
    fn default() -> TextRule {
        TextRule {
            ours_label: "ours".into(),
            theirs_label: "theirs".into(),
            strict: false,
        }
    }
}

impl TextRule {
    /// The merged bytes, under this rule.
    pub fn merge(
        &self,
        old: Option<&[u8]>,
        ours: Option<&[u8]>,
        theirs: Option<&[u8]>,
    ) -> Result<Vec<u8>, TextError> {
        let (merged, conflicted) =
            text_merge(old, ours, theirs, &self.ours_label, &self.theirs_label)?;
        if conflicted && self.strict {
            return Err(TextError::CantMark(
                "strict text merge: conflicting changes cannot be marked".into(),
            ));
        }
        Ok(merged)
    }
}

/// What `str.splitlines` ends a line at, a `\r\n` pair aside. A line
/// already ending in one is terminated.
fn is_boundary(c: char) -> bool {
    matches!(
        c,
        '\n' | '\r'
            | '\x0b'
            | '\x0c'
            | '\x1c'
            | '\x1d'
            | '\x1e'
            | '\u{85}'
            | '\u{2028}'
            | '\u{2029}'
    )
}

/// Check that neither label holds a line break.
pub fn check_labels(ours_label: &str, theirs_label: &str) -> Result<(), TextError> {
    for (name, label) in [("ours_label", ours_label), ("theirs_label", theirs_label)] {
        if label.chars().any(is_boundary) {
            return Err(TextError::BadLabel {
                name,
                label: label.to_string(),
            });
        }
    }
    Ok(())
}

/// The three-way merge of a value's sides, and whether it marked a
/// conflict. A side is `None` where the key is absent. The flag is what
/// the merge did, which counting `<<<<<<<` lines can't tell: the inputs
/// may hold marker-like lines that merge through untouched.
pub fn text_merge(
    old: Option<&[u8]>,
    ours: Option<&[u8]>,
    theirs: Option<&[u8]>,
    ours_label: &str,
    theirs_label: &str,
) -> Result<(Vec<u8>, bool), TextError> {
    check_labels(ours_label, theirs_label)?;
    let total: usize = [old, ours, theirs].iter().flatten().map(|d| d.len()).sum();
    if total > MAX_MARK_BYTES {
        return Err(TextError::CantMark(format!(
            "inputs total {total} bytes over cap {MAX_MARK_BYTES}"
        )));
    }
    let (base_text, our_text, their_text) = (decode(old)?, decode(ours)?, decode(theirs)?);
    let labels = Labels {
        ours: ours_label,
        theirs: theirs_label,
    };
    let (base, ours_lines, theirs_lines) = (
        split_lines(base_text),
        split_lines(our_text),
        split_lines(their_text),
    );
    let (lines, conflicted) = merge_lines(
        &base,
        &ours_lines,
        &theirs_lines,
        &labels,
        ours.is_none() && old.is_some(),
        theirs.is_none() && old.is_some(),
    );
    Ok((lines.concat().into_bytes(), conflicted))
}

/// One side as text; a removed side is empty, so markers show the
/// other side against nothing.
fn decode(data: Option<&[u8]>) -> Result<&str, TextError> {
    let Some(data) = data else { return Ok("") };
    if data.contains(&0) {
        return Err(TextError::CantMark("NUL byte: not text".into()));
    }
    std::str::from_utf8(data).map_err(|e| TextError::CantMark(format!("not UTF-8: {e}")))
}

/// Lines with their endings, as `str.splitlines(keepends=True)`: a
/// `\r\n` pair stays together, every other boundary ends a line, and a
/// final break leaves no empty line after it.
pub fn split_lines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if !is_boundary(c) {
            continue;
        }
        let mut end = i + c.len_utf8();
        if c == '\r' && chars.peek().is_some_and(|&(_, next)| next == '\n') {
            chars.next();
            end += 1;
        }
        lines.push(&text[start..end]);
        start = end;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

struct Labels<'a> {
    ours: &'a str,
    theirs: &'a str,
}

impl Labels<'_> {
    fn open(&self) -> String {
        format!("<<<<<<< {}\n", self.ours)
    }

    fn close(&self) -> String {
        format!(">>>>>>> {}\n", self.theirs)
    }
}

/// A line of the merge: one of the inputs', or a marker it wrote.
type Line<'a> = std::borrow::Cow<'a, str>;

/// One side's change to the base: `base[start..end]` becomes `lines`.
/// A pure insertion has `start == end`.
struct Hunk<'a> {
    ours: bool,
    start: usize,
    end: usize,
    lines: &'a [&'a str],
}

fn merge_lines<'a>(
    base: &'a [&'a str],
    ours: &'a [&'a str],
    theirs: &'a [&'a str],
    labels: &Labels<'_>,
    ours_deleted: bool,
    theirs_deleted: bool,
) -> (Vec<Line<'a>>, bool) {
    let borrowed = |lines: &[&'a str]| lines.iter().map(|&l| Line::Borrowed(l)).collect();
    if ours == theirs {
        return (borrowed(ours), false);
    }

    // A whole value removed on one side and changed on the other is a
    // conflict showing the survivor whole: hunks alone would drop its
    // unchanged lines.
    if ours_deleted && theirs != base {
        let mut out = vec![Line::Owned(labels.open()), Line::Borrowed("=======\n")];
        out.extend(theirs.iter().map(|&l| Line::Borrowed(l)));
        out.push(Line::Owned(labels.close()));
        return (terminate(out), true);
    }
    if theirs_deleted && ours != base {
        let mut out = vec![Line::Owned(labels.open())];
        out.extend(ours.iter().map(|&l| Line::Borrowed(l)));
        out.push(Line::Borrowed("=======\n"));
        out.push(Line::Owned(labels.close()));
        return (terminate(out), true);
    }

    let our_hunks = hunks(base, ours, true);
    let their_hunks = hunks(base, theirs, false);
    if our_hunks.is_empty() {
        return (borrowed(theirs), false);
    }
    if their_hunks.is_empty() {
        return (borrowed(ours), false);
    }

    // Overlapping hunks group into one region; disjoint ones resolve on
    // their own. Ranges are half-open over the base, so an insertion is
    // a point: it joins a group it falls strictly inside, or one holding
    // another insertion at the same point, but one merely touching a
    // change sits cleanly beside it. The sort is stable, so ours come
    // first among equals.
    let mut events: Vec<Hunk<'a>> = our_hunks.into_iter().chain(their_hunks).collect();
    events.sort_by_key(|h| (h.start, h.end));
    let mut groups: Vec<(Vec<Hunk<'a>>, usize)> = Vec::new();
    for hunk in events {
        if let Some((group, end)) = groups.last_mut() {
            let joins = hunk.start < *end
                || (hunk.start == hunk.end
                    && group
                        .iter()
                        .any(|g| g.start == g.end && g.start == hunk.start));
            if joins {
                *end = (*end).max(hunk.end);
                group.push(hunk);
                continue;
            }
        }
        let end = hunk.end;
        groups.push((vec![hunk], end));
    }

    let mut out: Vec<Line<'a>> = Vec::new();
    let mut conflicted = false;
    let mut pos = 0;
    for (group, _) in &groups {
        let start = group.iter().map(|h| h.start).min().unwrap_or(pos);
        let end = group.iter().map(|h| h.end).max().unwrap_or(pos);
        out.extend(base[pos..start].iter().map(|&l| Line::Borrowed(l)));
        let (our_side, their_side): (Vec<&Hunk<'a>>, Vec<&Hunk<'a>>) =
            group.iter().partition(|h| h.ours);
        if their_side.is_empty() {
            out.extend(replay(base, start, end, &our_side));
        } else if our_side.is_empty() {
            out.extend(replay(base, start, end, &their_side));
        } else {
            let our_full = replay(base, start, end, &our_side);
            let their_full = replay(base, start, end, &their_side);
            if our_full == their_full {
                out.extend(our_full);
            } else {
                conflicted = true;
                out.push(Line::Owned(labels.open()));
                out.extend(our_full);
                out.push(Line::Borrowed("=======\n"));
                out.extend(their_full);
                out.push(Line::Owned(labels.close()));
            }
        }
        pos = end;
    }
    out.extend(base[pos..].iter().map(|&l| Line::Borrowed(l)));
    (terminate(out), conflicted)
}

/// The changes `side` makes to `base`.
fn hunks<'a>(base: &[&'a str], side: &'a [&'a str], ours: bool) -> Vec<Hunk<'a>> {
    opcodes(base, side)
        .into_iter()
        .filter(|op| op.tag != Tag::Equal)
        .map(|op| Hunk {
            ours,
            start: op.a.0,
            end: op.a.1,
            lines: &side[op.b.0..op.b.1],
        })
        .collect()
}

/// One side's whole content over `base[start..end]`: the base's lines
/// outside its hunks, the hunks' lines inside. A side's hunks come from
/// one diff, so they never overlap.
fn replay<'a>(base: &[&'a str], start: usize, end: usize, hunks: &[&Hunk<'a>]) -> Vec<Line<'a>> {
    let mut ordered: Vec<&&Hunk<'a>> = hunks.iter().collect();
    ordered.sort_by_key(|h| (h.start, h.end));
    let mut out = Vec::new();
    let mut pos = start;
    for hunk in ordered {
        if hunk.start > pos {
            out.extend(base[pos..hunk.start].iter().map(|&l| Line::Borrowed(l)));
        }
        out.extend(hunk.lines.iter().map(|&l| Line::Borrowed(l)));
        pos = pos.max(hunk.end);
    }
    if end > pos {
        out.extend(base[pos..end].iter().map(|&l| Line::Borrowed(l)));
    }
    out
}

/// End every line but the last with a break, so no line runs into the
/// next. A line already ending in any boundary is left alone, and the
/// last keeps whatever ending it had.
fn terminate(mut lines: Vec<Line<'_>>) -> Vec<Line<'_>> {
    let last = lines.len().saturating_sub(1);
    for line in &mut lines[..last] {
        if !line.chars().next_back().is_some_and(is_boundary) {
            line.to_mut().push('\n');
        }
    }
    lines
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tag {
    Replace,
    Delete,
    Insert,
    Equal,
}

/// `a[a.0..a.1]` becomes `b[b.0..b.1]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Opcode {
    pub tag: Tag,
    pub a: (usize, usize),
    pub b: (usize, usize),
}

/// The edits that turn `a` into `b`, as difflib's
/// `SequenceMatcher(None, a, b, autojunk=False).get_opcodes()` gives
/// them.
pub fn opcodes<T: Eq + std::hash::Hash>(a: &[T], b: &[T]) -> Vec<Opcode> {
    let mut out = Vec::new();
    let (mut i, mut j) = (0, 0);
    for (ai, bj, size) in matching_blocks(a, b) {
        let tag = match (i < ai, j < bj) {
            (true, true) => Some(Tag::Replace),
            (true, false) => Some(Tag::Delete),
            (false, true) => Some(Tag::Insert),
            (false, false) => None,
        };
        if let Some(tag) = tag {
            out.push(Opcode {
                tag,
                a: (i, ai),
                b: (j, bj),
            });
        }
        (i, j) = (ai + size, bj + size);
        if size > 0 {
            out.push(Opcode {
                tag: Tag::Equal,
                a: (ai, i),
                b: (bj, j),
            });
        }
    }
    out
}

/// The matching runs `(i, j, n)`, `a[i..i+n] == b[j..j+n]`, in order,
/// adjacent runs joined, ending with `(a.len(), b.len(), 0)`.
fn matching_blocks<T: Eq + std::hash::Hash>(a: &[T], b: &[T]) -> Vec<(usize, usize, usize)> {
    let mut b2j: HashMap<&T, Vec<usize>> = HashMap::new();
    for (j, elt) in b.iter().enumerate() {
        b2j.entry(elt).or_default().push(j);
    }
    let mut queue = vec![(0, a.len(), 0, b.len())];
    let mut blocks = Vec::new();
    while let Some((alo, ahi, blo, bhi)) = queue.pop() {
        let (i, j, k) = longest_match(a, b, &b2j, alo, ahi, blo, bhi);
        if k > 0 {
            blocks.push((i, j, k));
            if alo < i && blo < j {
                queue.push((alo, i, blo, j));
            }
            if i + k < ahi && j + k < bhi {
                queue.push((i + k, ahi, j + k, bhi));
            }
        }
    }
    blocks.sort_unstable();
    let mut joined = Vec::new();
    let (mut i1, mut j1, mut k1) = (0, 0, 0);
    for (i2, j2, k2) in blocks {
        if i1 + k1 == i2 && j1 + k1 == j2 {
            k1 += k2;
        } else {
            if k1 > 0 {
                joined.push((i1, j1, k1));
            }
            (i1, j1, k1) = (i2, j2, k2);
        }
    }
    if k1 > 0 {
        joined.push((i1, j1, k1));
    }
    joined.push((a.len(), b.len(), 0));
    joined
}

/// The longest run `a[i..i+k] == b[j..j+k]` inside `a[alo..ahi]` and
/// `b[blo..bhi]`: of those, the one starting earliest in `a`, then
/// earliest in `b`.
fn longest_match<T: Eq + std::hash::Hash>(
    a: &[T],
    b: &[T],
    b2j: &HashMap<&T, Vec<usize>>,
    alo: usize,
    ahi: usize,
    blo: usize,
    bhi: usize,
) -> (usize, usize, usize) {
    let (mut besti, mut bestj, mut bestsize) = (alo, blo, 0);
    // The length of the run ending at each `j`, for the row before.
    let mut j2len: HashMap<usize, usize> = HashMap::new();
    for (i, elt) in a.iter().enumerate().take(ahi).skip(alo) {
        let mut next: HashMap<usize, usize> = HashMap::new();
        for &j in b2j.get(elt).map(Vec::as_slice).unwrap_or_default() {
            if j < blo {
                continue;
            }
            if j >= bhi {
                break;
            }
            let k = j
                .checked_sub(1)
                .and_then(|p| j2len.get(&p))
                .copied()
                .unwrap_or(0)
                + 1;
            next.insert(j, k);
            if k > bestsize {
                (besti, bestj, bestsize) = (i + 1 - k, j + 1 - k, k);
            }
        }
        j2len = next;
    }
    // With no junk, a longest run is already as wide as it goes; these
    // are difflib's extensions over equal neighbours, kept so the
    // answer is its answer.
    while besti > alo && bestj > blo && a[besti - 1] == b[bestj - 1] {
        (besti, bestj, bestsize) = (besti - 1, bestj - 1, bestsize + 1);
    }
    while besti + bestsize < ahi
        && bestj + bestsize < bhi
        && a[besti + bestsize] == b[bestj + bestsize]
    {
        bestsize += 1;
    }
    (besti, bestj, bestsize)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(old: &str, ours: &str, theirs: &str) -> (String, bool) {
        let (bytes, conflicted) = text_merge(
            Some(old.as_bytes()),
            Some(ours.as_bytes()),
            Some(theirs.as_bytes()),
            "ours",
            "theirs",
        )
        .unwrap();
        (String::from_utf8(bytes).unwrap(), conflicted)
    }

    #[test]
    fn disjoint_changes_merge_cleanly() {
        assert_eq!(
            merge("a\nb\nc\n", "A\nb\nc\n", "a\nb\nC\n"),
            ("A\nb\nC\n".into(), false)
        );
    }

    #[test]
    fn overlapping_changes_are_marked() {
        assert_eq!(
            merge("a\n", "ours\n", "theirs\n"),
            (
                "<<<<<<< ours\nours\n=======\ntheirs\n>>>>>>> theirs\n".into(),
                true
            )
        );
    }

    #[test]
    fn lines_split_as_python_splits_them() {
        assert_eq!(
            split_lines("a\r\nb\rc\u{2028}d"),
            ["a\r\n", "b\r", "c\u{2028}", "d"]
        );
        assert_eq!(split_lines("a\n"), ["a\n"]);
        assert!(split_lines("").is_empty());
    }

    #[test]
    fn unmarkable_values_are_refused() {
        let refused = |old: &[u8]| {
            matches!(
                text_merge(Some(old), Some(b"x"), Some(b"y"), "ours", "theirs"),
                Err(TextError::CantMark(_))
            )
        };
        assert!(refused(b"a\0b"));
        assert!(refused(b"\xff"));
        assert!(refused(&vec![b'a'; MAX_MARK_BYTES]));
    }

    #[test]
    fn a_label_with_a_break_is_the_callers_mistake() {
        assert!(matches!(
            text_merge(None, None, None, "a\rb", "theirs"),
            Err(TextError::BadLabel {
                name: "ours_label",
                ..
            })
        ));
    }

    #[test]
    fn opcodes_match_difflib() {
        let ops = opcodes(&["a", "b", "c", "d"], &["a", "x", "c", "d", "e"]);
        let tags: Vec<Tag> = ops.iter().map(|o| o.tag).collect();
        assert_eq!(tags, [Tag::Equal, Tag::Replace, Tag::Equal, Tag::Insert]);
        assert_eq!(ops[1].a, (1, 2));
        assert_eq!(ops[3].b, (4, 5));
    }
}
