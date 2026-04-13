# Enterprise Reporting Sample Project

This project template is for the IngenFab Enterprise Reporting tutorial.
It models a **Supply Chain & Logistics** domain with a star schema designed for Power BI reporting.

## Architecture

- **lh_bronze** — Raw shipment, order, product, supplier, and region data (populated via synthetic data generation)
- **lh_silver** — Cleaned and deduplicated data
- **lh_gold** — Star schema: `fact_shipments` + 4 dimension tables
- **wh_reporting** — T-SQL views on top of gold tables for Fabric semantic models

## Quick Start

See the [Enterprise Reporting Tutorial](../../docs/training/er/index.md).
