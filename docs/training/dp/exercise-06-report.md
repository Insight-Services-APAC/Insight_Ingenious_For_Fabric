# Exercise 6 — Power BI Report

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 6

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Power BI report in Fabric connected to a semantic model
- Build basic visuals using the geography dataset
- Apply slicers and filters
- Save and share the report within the workspace

## Prerequisites

- [Exercise 5](exercise-05-semantic-model.md) completed — `sm_dp_geography` published to the workspace

## Background

In Fabric, a **Power BI report** is a separate item from the semantic model. Reports are built on top of semantic models — one model can power many reports. Because the semantic model uses DirectQuery against `wh_gold`, all report visuals query live data.

## Steps

### 1. Create a new report

1. In the Fabric workspace, find `sm_dp_geography`
2. Click the **…** menu → **Create report**
3. This opens the Power BI report editor connected to your semantic model

### 2. Build a population bar chart

1. In the **Visualisations** pane, select **Clustered bar chart**
2. Set the fields:
   - **Y-axis**: `vDim_Cities[CityName]`
   - **X-axis**: `[Total Population]` (your measure)
3. Sort descending by `Total Population`

### 3. Add a country slicer

If you have `vDim_CityGeography` in the model:

1. Add a **Slicer** visual
2. Set **Field** to `vDim_CityGeography[CountryName]`
3. Selecting a country cross-filters the bar chart

### 4. Add a summary card

1. Add a **Card** visual
2. Set **Field** to `[City Count]`
3. Place it in the report header

### 5. Add a detail table

1. Add a **Table** visual
2. Include columns: `CityName`, `StateProvinceName` *(if available)*, `CountryName` *(if available)*, `LatestRecordedPopulation`

### 6. Save the report

1. Click **File** → **Save**
2. Name it `rpt_dp_city_geography`
3. Save to the current workspace

## Verification

1. `rpt_dp_city_geography` appears as a report item in the workspace
2. The bar chart shows city names with population values
3. The city count card shows `10`
4. The country slicer filters the chart correctly (if added)

## Notes

- Reports saved in Fabric can be shared via link, embedded in apps, or set up for email subscriptions
- The report definition can be downloaded for source control via `ingen_fab deploy download-artefact -n "rpt_dp_city_geography" -t Report`

---

← [Exercise 5 — Semantic Model](exercise-05-semantic-model.md) | **Next:** [Exercise 7 — GraphQL API →](exercise-07-graphql.md) *(Stretch)*
