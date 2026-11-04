# Security

## Reporting

Open a private security advisory at <https://github.com/furey/fetchtv/security/advisories/new>.

## Dependency advisories

`npm audit --omit=dev` reports no advisories for the production dependencies.

- fetchtv sends its own SSDP `M-SEARCH` with Node's `dgram` module. It no longer depends on `node-ssdp`, so the `ip` package (GHSA-2p57-rm9w-gvfp, no fixed version) is not in the dependency tree.
- `fast-xml-parser` is on the `5.x` line, which fixes GHSA-gh4j-gqv2-49f6. `test/xml-golden.test.js` compares the parsed Fetch box XML and the recording listings with a snapshot from the `4.x` parser.
