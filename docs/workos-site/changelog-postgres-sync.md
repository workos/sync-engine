<!--
Proposed changelog entry for workos.com/changelog

Slug:  postgres-sync
Date:  August 6, 2026
Body:  73 words, one paragraph, closing docs CTA — matching the house format
       (see "Stripe Billing Seat Sync" and "API Keys" for the closest analogues).
-->

# Postgres Sync

**August 6, 2026**

Mirror your WorkOS data into your own Postgres database with the new sync
engine. It backfills existing users, organizations, memberships, and directory
data, then follows the events API to stay current, so you can query everything
with SQL and join it to your own tables. A reconciliation command compares the
mirror against the API and repairs any drift, including deletions. Get started
with Postgres Sync in the docs.

---

## Index summary

*(one-sentence summary shown on the changelog index page, above "Read more")*

Mirror your WorkOS users, organizations, and directory data into your own Postgres database, and query it with SQL.

---

## Hero image

**Specification:** 1690×888 WebP, plus a 1200×630 OG variant. Matches the
established changelog illustration style — abstract conceptual diagram, never a
product screenshot.

**Composition.** A left-to-right data-flow topology on a near-black field
(approximately `#080B14`) with a soft blue radial glow centered behind the
midpoint of the diagram.

- **Left:** a large glassmorphic rounded-rectangle card containing the WorkOS
  logo and wordmark, with a thin 1px light border and a subtle diagonal
  gradient sweep across its surface. Beneath the logo, small pale-lavender
  labels reading `users`, `organizations`, `directory`, stacked and
  left-aligned, suggesting the objects being mirrored.

- **Center:** three thin orthogonal connector lines running right from the
  WorkOS card, routed at right angles rather than curves, each carrying a
  pink → lavender → blue gradient along its length. Small dotted particle
  streams along the lines convey direction of flow — left to right only, since
  the sync is one-directional. This is the one place to allow a little motion
  in a still image.

- **Right:** a dark rounded-square tile holding the Postgres elephant logo in
  full color, in the same treatment used for third-party provider logos in the
  Pipes illustration. Directly below or beside it, a second rounded-rect card
  rendering a stylized "rows of data" glyph: four or five horizontal bars of
  varying width in pale lavender, reading as table rows, with the topmost bar
  brightest to suggest a header.

- **Type:** clean grotesque sans, generous size, white and pale lavender only.
  Card labels only — no sentences.

**Palette:** near-black background, white, pale lavender, hot pink, periwinkle
blue. Brand logos retain their own colors inside their tiles.

**What to avoid:** no screenshots, no terminal windows, no SQL text, no
bidirectional arrows (the sync is one-way and the image should not imply
otherwise), and no database-cylinder cliché — the rows-of-data glyph carries
the idea better and matches the existing visual language.

---

## Contributors

*(byline list with avatars, per house format)*
