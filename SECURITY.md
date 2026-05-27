# Security

## Reporting

Open a private security advisory at https://github.com/furey/fetchtv/security/advisories/new.

## Accepted Residual Risks

After bumping `axios`, `fast-xml-parser`, and `lodash` for the 2026-05 Dependabot sweep, two transitive advisories remain. Both have been reviewed and accepted because they do not affect fetchtv's code paths.

### `ip` — GHSA-2p57-rm9w-gvfp (SSRF via `isPublic` miscategorisation)

- Pulled in transitively by `node-ssdp@4.0.1`.
- No upstream patch exists. `ip@2.0.1` is still affected per the advisory.
- fetchtv never calls `ip.isPublic`. SSDP is used only for LAN discovery of the Fetch TV box; discovered locations are HTTP URLs on the local network and are not passed through `ip.isPublic`.
- Remediation would require replacing `node-ssdp`. The available alternatives have not been validated against Fetch TV's UPnP advertisement format, so the migration is deferred.

### `fast-xml-parser` — GHSA-gh4j-gqv2-49f6 (XMLBuilder comment/CDATA injection)

- Affects `XMLBuilder` only. fetchtv imports `XMLParser` and `XMLValidator`, never `XMLBuilder`.
- The 4.x line will not receive a fix; only `fast-xml-parser@5.x` patches it. Bumping to 5.x is unnecessary work given we don't touch the affected surface.
