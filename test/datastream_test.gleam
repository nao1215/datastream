import datastream
import gleeunit
import gleeunit/should

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn version_test() {
  datastream.version()
  |> should.equal("0.1.0")
}
