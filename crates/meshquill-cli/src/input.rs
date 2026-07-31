use std::io::{self, BufRead, Read as _};

use crate::{error::CliError, output::ExitStatus};

pub(crate) const MAX_INTERACTIVE_LINE_BYTES: usize = 4 * 1024;

pub(crate) fn read_bounded_line(
    input: &mut impl BufRead,
    context: &'static str,
) -> Result<Option<String>, CliError> {
    let read_bound = u64::try_from(MAX_INTERACTIVE_LINE_BYTES)
        .unwrap_or(u64::MAX)
        .saturating_add(3);
    let mut bytes = Vec::with_capacity(MAX_INTERACTIVE_LINE_BYTES.saturating_add(2));
    let read = input
        .take(read_bound)
        .read_until(b'\n', &mut bytes)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::Interrupted {
                CliError::new(
                    ExitStatus::Interrupted,
                    format!("{context} was interrupted"),
                )
            } else {
                CliError::new(ExitStatus::Protocol, format!("could not read {context}"))
            }
        })?;
    if read == 0 {
        return Ok(None);
    }
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    if bytes.len() > MAX_INTERACTIVE_LINE_BYTES {
        return Err(CliError::new(
            ExitStatus::Usage,
            format!("{context} exceeds the {MAX_INTERACTIVE_LINE_BYTES}-byte input bound"),
        ));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|_| CliError::new(ExitStatus::Usage, format!("{context} must be valid UTF-8")))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn line_bound_is_inclusive_and_counts_utf8_bytes() {
        let exact = format!("{}\r\n", "é".repeat(MAX_INTERACTIVE_LINE_BYTES / 2));
        let mut input = Cursor::new(exact.as_bytes());
        let line = read_bounded_line(&mut input, "test input")
            .expect("exact-bound line")
            .expect("line");
        assert_eq!(line.len(), MAX_INTERACTIVE_LINE_BYTES);

        let oversized = format!("{}\n", "x".repeat(MAX_INTERACTIVE_LINE_BYTES + 1));
        let mut input = Cursor::new(oversized.as_bytes());
        assert!(read_bounded_line(&mut input, "test input").is_err());
    }

    #[test]
    fn line_reader_rejects_invalid_utf8_and_reports_eof() {
        let mut invalid = Cursor::new([0xff, b'\n']);
        assert!(read_bounded_line(&mut invalid, "test input").is_err());
        let mut empty = Cursor::new(Vec::<u8>::new());
        assert_eq!(
            read_bounded_line(&mut empty, "test input").expect("EOF"),
            None
        );
    }
}
