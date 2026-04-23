//// Optional integration helpers between `datastream` and the
//// `dataprep` validation library.
////
//// Two helpers, both full-traversal: applicative error accumulation
//// is incompatible with early exit because the whole point is to
//// surface every failure from one pass.
////
//// The core `datastream` API does not depend on `dataprep`. Only
//// callers who import this module pay the dependency cost.

import dataprep/non_empty_list.{type NonEmptyList}
import dataprep/validated.{type Validated, Invalid, Valid}
import datastream.{type Stream}
import datastream/fold
import gleam/list

/// Accumulate every element of the stream into a single
/// `Validated(List(a), e)`.
///
/// Returns `Valid(values_in_order)` when every element is `Valid`.
/// On any `Invalid`, returns `Invalid(errors_accumulated_in_order)` —
/// errors from later `Invalid` elements append to the running
/// error list, preserving source order across all failures.
///
/// Empty stream → `Valid([])`.
///
/// Drives the entire stream; never short-circuits.
pub fn collect_validated(
  over stream: Stream(Validated(a, e)),
) -> Validated(List(a), e) {
  let folded = fold.fold(over: stream, from: Valid([]), with: collect_step)
  case folded {
    Valid(values) -> Valid(list.reverse(values))
    Invalid(errors) -> Invalid(errors)
  }
}

fn collect_step(
  acc: Validated(List(a), e),
  next: Validated(a, e),
) -> Validated(List(a), e) {
  case acc, next {
    Valid(values), Valid(value) -> Valid([value, ..values])
    Valid(_), Invalid(errors) -> Invalid(errors)
    Invalid(acc_errors), Valid(_) -> Invalid(acc_errors)
    Invalid(acc_errors), Invalid(errors) ->
      Invalid(non_empty_list.append(acc_errors, errors))
  }
}

/// Split the stream into a list of valid values and a list of
/// per-element error groups, both in source order.
///
/// Each `Invalid(NonEmptyList(e))` element contributes one entry to
/// the right-hand list, keeping the per-element error grouping that
/// the validation library guarantees on its `Invalid` constructor.
///
/// Drives the entire stream; never short-circuits.
pub fn partition_validated(
  over stream: Stream(Validated(a, e)),
) -> #(List(a), List(NonEmptyList(e))) {
  let #(valids, invalids) =
    fold.fold(over: stream, from: #([], []), with: partition_step)
  #(list.reverse(valids), list.reverse(invalids))
}

fn partition_step(
  acc: #(List(a), List(NonEmptyList(e))),
  next: Validated(a, e),
) -> #(List(a), List(NonEmptyList(e))) {
  let #(valids, invalids) = acc
  case next {
    Valid(value) -> #([value, ..valids], invalids)
    Invalid(errors) -> #(valids, [errors, ..invalids])
  }
}
