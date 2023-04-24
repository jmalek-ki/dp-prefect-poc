import functools

from prefect import flow, task
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse


# This is a 'library' of CTEs for OEP queries.
# You can SELECT from any stage of the pipeline, although you likely
OEP_CTE_LIBRARY = """
WITH
  -- Aux. CTEs to dump a subset of data to save costs --
  raw_data_dump_nonki AS (
    SELECT
      *
    FROM `ki-datalake-nonprod-a7e5.air_live.OpenMarketLoss`
    WHERE
      TRIM(CatalogTypeCode) = "STC" AND
      EntityName != "S1618"
  ),

  raw_data_dump_ki AS (
    SELECT
      *
    FROM `ki-datalake-nonprod-a7e5.air_live.OpenMarketLoss`
    WHERE
      TRIM(CatalogTypeCode) = "STC" AND
      EntityName = "S1618"
  ),

  raw_data_dump AS (
    SELECT * FROM raw_data_dump_nonki
    UNION ALL
    SELECT * FROM raw_data_dump_ki
  ),

  -- BEGIN ACTUAL LOGIC --
  raw_AIR_loss_data AS (
    SELECT
      PolicyReference As PolicyRef,
      EntityName,
      Event,
      YOA,
      Year,
      TRIM(CatalogTypeCode) As CatalogTypeCode,
      PerilCode,
      GrossLoss,
    FROM `ki-datalake-nonprod-a7e5.air_live.OpenMarketLoss`
  ),

  stc_filtered_AIR_loss AS (
    -- filter on stochastic modelling
    SELECT * FROM raw_AIR_loss_data
    WHERE CatalogTypeCode = "STC"
  ),

  policies_with_quoteline AS (
    SELECT *
    FROM `ki-datalake-prod-c82c.crm_live.Policies`
    WHERE QuoteLineId IS NOT NULL
  ),

  policy_view AS (
    SELECT *
    FROM `ki-datalake-prod-c82c.ki_datamarts_live.PolicyView`
    WHERE PolicyRef IS NOT NULL
  ),

  quotes_with_quoteline AS (
    SELECT *
    FROM `ki-datalake-nonprod-a7e5.ki_datamarts_dev.QuoteView`
    WHERE QuoteLineId IS NOT NULL
  ),

  AirLossJoinPoliciesAndPolicyView AS (
    SELECT
      PolicyRef,
      EntityName,
      Event,
      stc_filtered_AIR_loss.YOA,
      Year,
      CatalogTypeCode,
      PerilCode,
      GrossLoss,
      pview.QuoteLineId,
      pview.Exposure_USD as LineSize_USD,
      pview.GrossNetWrittenPremium as GrossNetWrittenPremium_USD,
      pview.GNWPDueToRARC_USD as GrossNetWrittenPremiumDueToRARC_USD
    FROM stc_filtered_AIR_loss
    JOIN policies_with_quoteline pol
    USING (PolicyRef)
    JOIN policy_view pview
    USING (PolicyRef)
  ),

  AirLossJoinPoliciesPolicyViewAndQuotes AS (
    -- This can be `SELECT *`d from into an intermediate table.
    -- This gives the query planner a breather; otherwise, the plan is too complex (!) for BQ
    SELECT
      PolicyRef,
      EntityName,
      Event,
      GroupClass,
      AirLossJoinPoliciesAndPolicyView.YOA,
      Year,
      CatalogTypeCode,
      PerilCode,
      GrossLoss,
      QuoteLineId,
      LineSize_USD,
      GrossNetWrittenPremium_USD,
      GrossNetWrittenPremiumDueToRARC_USD,
      GrossNetWrittenPremium_USD - COALESCE(GrossNetWrittenPremiumDueToRARC_USD, 0) AS GrossNetWrittenPremiumExRARC_USD,
      COALESCE(qts.FloridaTIVUSD, 0.0) AS FloridaTIVUSD,
      COALESCE(qts.CaliTIVUSD, 0.0) AS CaliforniaTIVUSD,
      COALESCE(qts.FloridaTIVUSD, 0.0) > 0 AS FloridaExposed,
      COALESCE(qts.CaliTIVUSD, 0.0) > 0 AS CaliforniaExposed
    FROM AirLossJoinPoliciesAndPolicyView
    JOIN quotes_with_quoteline qts
    USING (QuoteLineId)
    WHERE PerilCode = "WS" OR PerilCode = "EQ"
  ),

  AirLossJoinPoliciesPolicyViewAndQuotesFromCached AS (
    -- This is the same as AirLossJoinPoliciesPolicyViewAndQuotes, except we've precomputed
    -- the necessary data to a table already.
    SELECT
      PolicyRef,
      EntityName,
      Event,
      GroupClass,
      Year,
      CatalogTypeCode,
      PerilCode,
      GrossLoss,
      QuoteLineId,
      LineSize_USD,
      GrossNetWrittenPremium_USD,
      GrossNetWrittenPremiumDueToRARC_USD,
      GrossNetWrittenPremiumExRARC_USD,
      FloridaTIVUSD,
      CaliforniaTIVUSD,
      FloridaExposed,
      CaliforniaExposed
    FROM `ki-datalake-nonprod-a7e5.sandbox. oep_etl_AIRloss_joined_view_full_13Apr23`
  ),

  KiOnlyLoss AS (
    -- CAT modelling for Ki
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY PolicyRef, GroupClass, Year, Event ORDER BY PolicyRef DESC) AS CandRank
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    WHERE EntityName = "S1618"
  ),

  KiOnlyLossDeduped AS (
    SELECT
      PolicyRef,
      GroupClass,
      Event,
      PerilCode,
      Year,
      LineSize_USD AS LineSize_USD,
      FloridaExposed AS FloridaExposed,
      CaliforniaExposed AS CaliforniaExposed,
      FloridaTIVUSD AS FloridaTIVUSD,
      CaliforniaTIVUSD AS CaliforniaTIVUSD,
      GrossLoss AS GrossLoss,
      EntityName,
      ROW_NUMBER() OVER(PARTITION BY PolicyRef, GroupClass, Year, Event ORDER BY PolicyRef DESC) AS DedupedCandRank
    FROM KiOnlyLoss
    WHERE CandRank <= 1
  ),

  BritOnlyLoss AS (
    -- CAT modelling for Brit
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY PolicyRef, GroupClass, Year, Event ORDER BY PolicyRef DESC) AS CandRank
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    WHERE EntityName = "S2987"
  ),

  BritOnlyLossDeduped AS (
    SELECT
      PolicyRef,
      GroupClass,
      Event,
      PerilCode,
      Year,
      LineSize_USD AS LineSize_USD,
      FloridaExposed AS FloridaExposed,
      CaliforniaExposed AS CaliforniaExposed,
      FloridaTIVUSD AS FloridaTIVUSD,
      CaliforniaTIVUSD AS CaliforniaTIVUSD,
      GrossLoss AS GrossLoss,
      EntityName,
      ROW_NUMBER() OVER(PARTITION BY PolicyRef, GroupClass, Year, Event ORDER BY PolicyRef DESC) AS DedupedCandRank
    FROM BritOnlyLoss
    WHERE CandRank <= 1
  ),

  LivePolicies AS (
    -- Actual policies we have live as of the query run window
    SELECT
      PolicyRef,
      -- Year,
      Syndicate,
      ExpiryDate,
      GroupClass,
      UniqueMarketRef,
      Exposure_USD,
      GrossNetWrittenPremium,
      GNWPDueToRARC_USD,
    FROM policy_view
    -- NOTE: replace the date with a target window here:
    WHERE ExpiryDate >= '2023-01-01'
  ),

  KiMatchingReverse AS (
    -- Find live policies POSSIBLY with modelling attached
    SELECT
      LivePolicies.PolicyRef,
      -- LivePolicies.Year,
      LivePolicies.ExpiryDate,
      LivePolicies.GroupClass,
      LivePolicies.Exposure_USD,
      LivePolicies.GrossNetWrittenPremium,
      LivePolicies.GNWPDueToRARC_USD,
      -- random field from the right side of the join:
      KiOnlyLossDeduped.Year,
      KiOnlyLossDeduped.EntityName,
      KiOnlyLossDeduped.EntityName IS NOT NULL AS ModellingMatched
    FROM LivePolicies
    LEFT JOIN KiOnlyLossDeduped
    ON (
        LivePolicies.PolicyRef = KiOnlyLossDeduped.PolicyRef
        AND
        KiOnlyLossDeduped.DedupedCandRank <= 1
    )
    WHERE LivePolicies.Syndicate = "1618" OR LivePolicies.Syndicate = "2987"
  ),

  KiMatchingReverseMissing AS (
    -- Live policies that were not directly modelled, need estimating
    SELECT
      CONCAT(LEFT(PolicyRef, 6), "__", RIGHT(PolicyRef, 4)) AS FuzzyPolicyRef,
      *
    FROM KiMatchingReverse
    WHERE ModellingMatched = FALSE
  ),

  FuzzedPolicyRefKiLoss AS (
    -- Enriches the Ki loss with an extra field representing PolicyRef with the year
    -- masked for fuzzy matching with the previous year's policies that were renewed.
    SELECT
      CONCAT(LEFT(PolicyRef, 6), "__", RIGHT(PolicyRef, 4)) AS FuzzyPolicyRef,
      *
    FROM KiOnlyLoss
  ),

  RenewalFuzzyMatched AS (
    -- Renewal w/ changed year in PolicyRef - we can reuse modelling
    -- NOTE: Currently does NOT ensure the match ref's year is HIGHER than the
    --       matched policy's, so you could see 'time-travelling' matches!
    SELECT
      -- 'true' PolicyRef, for comparison:
      KiMatchingReverseMissing.PolicyRef AS NonestimatedPolicyRef,
      FuzzedPolicyRefKiLoss.PolicyRef As ModelledPolicyRef,
      -- Year:
      KiMatchingReverseMissing.Year As NonestimatedYear,
      FuzzedPolicyRefKiLoss.Year As ModelledYear,
      -- Group Class:
      KiMatchingReverseMissing.GroupClass As NonestimatedGroupClass,
      FuzzedPolicyRefKiLoss.GroupClass As ModelledGroupClass,
      -- Line Size:
      KiMatchingReverseMissing.Exposure_USD As NonestimatedLineSize_USD,
      FuzzedPolicyRefKiLoss.LineSize_USD As ModelledLineSize_USD,
      -- GNWP:
      FuzzedPolicyRefKiLoss.GrossNetWrittenPremiumExRARC_USD AS GnwpExRARC_USD,
      FuzzedPolicyRefKiLoss.GrossNetWrittenPremiumExRARC_USD * (KiMatchingReverseMissing.Exposure_USD / FuzzedPolicyRefKiLoss.LineSize_USD) AS LineSizeAdjusted_GnwpExRARC_USD,
      -- Gross Loss:
      FuzzedPolicyRefKiLoss.GrossLoss AS GrossLoss,
      FuzzedPolicyRefKiLoss.GrossLoss * (KiMatchingReverseMissing.Exposure_USD / FuzzedPolicyRefKiLoss.LineSize_USD) AS LineSizeAdjusted_GrossLoss,
      -- All the rest:
      KiMatchingReverseMissing.* EXCEPT (PolicyRef, Year, GroupClass, Exposure_USD),
      FuzzedPolicyRefKiLoss.* EXCEPT (PolicyRef, Year, GroupClass, LineSize_USD, GrossNetWrittenPremiumExRARC_USD, GrossLoss)

    FROM KiMatchingReverseMissing
    JOIN FuzzedPolicyRefKiLoss
    ON (
        KiMatchingReverseMissing.FuzzyPolicyRef = FuzzedPolicyRefKiLoss.FuzzyPolicyRef
        -- NOTE: Disabled for Dev, as we have no matching tuples in the sample.
        -- AND KiMatchingReverseMissing.Year > FuzzedPolicyRefKiLoss.Year
    )
  ),

  BritRenewalMatching AS (
    -- Straightforward matching of live policies to Brit CATs
    SELECT
      LivePolicies.PolicyRef,
      -- LivePolicies.Year,
      LivePolicies.ExpiryDate,
      LivePolicies.GroupClass,
      LivePolicies.Exposure_USD,
      LivePolicies.GrossNetWrittenPremium,
      LivePolicies.GNWPDueToRARC_USD,
      BritOnlyLossDeduped.Year,
      BritOnlyLossDeduped.Event,
      BritOnlyLossDeduped.PerilCode,
      BritOnlyLossDeduped.LineSize_USD,
      BritOnlyLossDeduped.FloridaTIVUSD,
      BritOnlyLossDeduped.CaliforniaTIVUSD,
      BritOnlyLossDeduped.FloridaExposed,
      BritOnlyLossDeduped.CaliforniaExposed,
      BritOnlyLossDeduped.GrossLoss,
      IF(BritOnlyLossDeduped.LineSize_USD > 0, (BritOnlyLossDeduped.GrossLoss / BritOnlyLossDeduped.LineSize_USD), 0.) * LivePolicies.Exposure_USD AS LossRescaledToKiLine,
      BritOnlyLossDeduped.EntityName IS NOT NULL AS ModellingMatched
    FROM LivePolicies
    LEFT JOIN BritOnlyLossDeduped
    USING (PolicyRef)
  ),

  BritRenewalMatched AS (
    -- Only those policies WITH a corresponding Brit CAT modelling
    SELECT
        *,
        GrossNetWrittenPremium - GNWPDueToRARC_USD AS GrossNetWrittenPremiumNoRARC,
    FROM BritRenewalMatching
    WHERE ModellingMatched
  ),

  KiRenewalMatching AS (
    -- Straightforward matching of live policies to Ki CATs
    SELECT
      LivePolicies.PolicyRef,
      -- LivePolicies.Year,
      LivePolicies.ExpiryDate,
      LivePolicies.GroupClass,
      LivePolicies.Exposure_USD,
      LivePolicies.GrossNetWrittenPremium,
      LivePolicies.GNWPDueToRARC_USD,
      (LivePolicies.GrossNetWrittenPremium - LivePolicies.GNWPDueToRARC_USD) AS GrossNetWrittenPremiumNoRARC,
      KiOnlyLossDeduped.Year,
      KiOnlyLossDeduped.Event,
      KiOnlyLossDeduped.PerilCode,
      KiOnlyLossDeduped.LineSize_USD,
      KiOnlyLossDeduped.FloridaTIVUSD,
      KiOnlyLossDeduped.CaliforniaTIVUSD,
      KiOnlyLossDeduped.FloridaExposed,
      KiOnlyLossDeduped.CaliforniaExposed,
      KiOnlyLossDeduped.GrossLoss,
      IF(KiOnlyLossDeduped.LineSize_USD > 0, (KiOnlyLossDeduped.GrossLoss / KiOnlyLossDeduped.LineSize_USD), 0.) * LivePolicies.Exposure_USD AS LossRescaledToKiLine,
      KiOnlyLossDeduped.EntityName IS NOT NULL AS ModellingMatched
    FROM LivePolicies
    LEFT JOIN KiOnlyLossDeduped
    USING (PolicyRef)
  ),

  KiRenewalMatched AS (
    -- Only those policies WITH a corresponding Ki CAT modelling
    SELECT *
    FROM KiRenewalMatching
    WHERE ModellingMatched
  ),

  KiRenewalFuzzyMatched AS (
    -- Only those policies WITHOUT a corresponding Ki CAT modelling in this year but WITH modelling from last year
    SELECT
      NonestimatedPolicyRef AS PolicyRef,
      Event,
      PerilCode,
      FloridaExposed,
      CaliforniaExposed,
      ModelledYear as Year,
      ExpiryDate,
      NonestimatedGroupClass as GroupClass,
      NonestimatedLineSize_USD,
      ModelledLineSize_USD,
      GrossNetWrittenPremium AS GrossNetWrittenPremium,
      GnwpExRARC_USD AS GrossNetWrittenPremiumNoRARC,
      LineSizeAdjusted_GnwpExRARC_USD AS GrossNetWrittenPremiumNoRARCwithRescale,
      GrossLoss,
      LineSizeAdjusted_GrossLoss
    FROM RenewalFuzzyMatched
  ),

  bucketStageKiModelled AS (
    -- Aggregation for estimating Ki with existing modelling
    SELECT
      KiRenewalMatched.Year,
      Event,
      PerilCode,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      -- SUM(GrossLoss) AS ModellingGrossLoss,
      -- NOTE: this rescaling actually does nothing in this case, just keeping things uniform:
      SUM(LossRescaledToKiLine) AS GrossLoss,
      SUM(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC,
      "Ki" AS SourceTag
    FROM KiRenewalMatched
    GROUP BY Year, Event, PerilCode, GroupClass, FloridaExposed, CaliforniaExposed
  ),

  bucketStageKiFuzzyMatched AS (
    -- Aggregation for estimating Ki with existing modelling
    SELECT
      Year,
      Event,
      PerilCode,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      -- SUM(GrossLoss) AS ModellingGrossLoss,
      SUM(GrossLoss) AS GrossLoss,
      SUM(GrossNetWrittenPremiumNoRARCwithRescale) AS GrossNetWrittenPremiumNoRARC,
      "FuzzyMatch" AS SourceTag
    FROM KiRenewalFuzzyMatched
    GROUP BY Year, Event, PerilCode, GroupClass, FloridaExposed, CaliforniaExposed
  ),

  bucketStageBrit AS (
    -- Aggregation for estimating Ki new business that are Brit renewals
    SELECT
      BritRenewalMatched.Year,
      Event,
      PerilCode,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      --SUM(GrossLoss) AS ModellingGrossLoss,
      SUM(LossRescaledToKiLine) AS GrossLoss,
      SUM(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC,
      "Brit" AS SourceTag
    FROM BritRenewalMatched
    GROUP BY Year, Event, PerilCode, GroupClass, FloridaExposed, CaliforniaExposed
  ),

  bucketStageUnified AS (
      SELECT * FROM bucketStageKiModelled
    UNION ALL
      SELECT * FROM bucketStageKiFuzzyMatched
    UNION ALL
      SELECT * FROM bucketStageBrit
  ),

  UnifiedAggregated AS (
    SELECT
      Year,
      Event,
      PerilCode,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      SUM(GrossLoss) AS GrossLoss,
      SUM(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC,
      ROW_NUMBER() OVER (PARTITION BY Year, Event, GroupClass, FloridaExposed, CaliforniaExposed) AS AggregationRank
    FROM bucketStageUnified
    WHERE GrossLoss IS NOT NULL AND GrossNetWrittenPremiumNoRARC IS NOT NULL
    GROUP BY Year, Event, PerilCode, GroupClass, FloridaExposed, CaliforniaExposed
  ),

  UnifiedAggregatedDeduped AS (
    SELECT
        Year,
        Event,
        GroupClass,
        FloridaExposed,
        CaliforniaExposed,
        AVG(GrossLoss) AS GrossLoss,
        AVG(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC
    FROM UnifiedAggregated
    GROUP BY Year, Event, GroupClass, FloridaExposed, CaliforniaExposed
  ),

  KiRenewalUnmatched AS (
    -- Only those policies WITHOUT a corresponding Ki CAT modelling
    SELECT
        PolicyRef,
        --Year,
        GroupClass,
        Event,
        GrossNetWrittenPremium,
        GNWPDueToRARC_USD,
        GrossNetWrittenPremiumNoRARC
        -- everything else is NULL by definition (no match)
    FROM KiRenewalMatching
    WHERE NOT ModellingMatched
  ),

  KiRenewalUnmatchedJoinPoliciesPolicyViewAndQuotes AS (
    SELECT
      KiRenewalUnmatched.*,
      pol.QuoteLineId,
      COALESCE(qts.FloridaTIVUSD, 0.0) AS FloridaTIVUSD,
      COALESCE(qts.CaliTIVUSD, 0.0) AS CaliforniaTIVUSD,
      COALESCE(qts.FloridaTIVUSD, 0.0) > 0 AS FloridaExposed,
      COALESCE(qts.CaliTIVUSD, 0.0) > 0 AS CaliforniaExposed,
    FROM KiRenewalUnmatched
    JOIN policies_with_quoteline pol
    USING (PolicyRef)
    JOIN quotes_with_quoteline qts
    USING (QuoteLineId)
  ),

  Analogous AS (
    -- Match up live policies with modelling buckets to estimate the loss (scaled by GNWP)
    SELECT
      PolicyRef,
      UnifiedAggregatedDeduped.Year,
      UnifiedAggregatedDeduped.Event,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      KiRenewalUnmatchedJoinPoliciesPolicyViewAndQuotes.GrossNetWrittenPremiumNoRARC AS GrossNetWrittenPremiumNoRARC,
      UnifiedAggregatedDeduped.GrossLoss AS ModelledLoss,
      UnifiedAggregatedDeduped.GrossNetWrittenPremiumNoRARC AS ModelledGNWP,
      AVG(
          UnifiedAggregatedDeduped.GrossLoss
            / UnifiedAggregatedDeduped.GrossNetWrittenPremiumNoRARC
      ) OVER (PARTITION BY UnifiedAggregatedDeduped.Year, GroupClass, FloridaExposed, CaliforniaExposed) * KiRenewalUnmatchedJoinPoliciesPolicyViewAndQuotes.GrossNetWrittenPremiumNoRARC AS EstimatedLoss,
      ROW_NUMBER() OVER (PARTITION BY PolicyRef, UnifiedAggregatedDeduped.Year, GroupClass, FloridaExposed, CaliforniaExposed) As DuplicatePolicyIdx

    FROM KiRenewalUnmatchedJoinPoliciesPolicyViewAndQuotes
    LEFT JOIN UnifiedAggregatedDeduped
    USING (GroupClass, FloridaExposed, CaliforniaExposed)
  ),

  AnalogousDeduped AS (
    SELECT
      PolicyRef,
      Year,
      Event,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      ModelledLoss,
      ModelledGNWP,
      GrossNetWrittenPremiumNoRARC,
      EstimatedLoss
    FROM Analogous
    WHERE DuplicatePolicyIdx < 2
  ),

  CombinedEstimatedLossPerPolicy AS (
        SELECT
          PolicyRef,
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          GrossNetWrittenPremiumNoRARC,
          EstimatedLoss AS GrossLoss,
          EstimatedLoss / GrossNetWrittenPremiumNoRARC AS PercentageLoss,
          "Bucket Estimate" AS EstimateTag
        FROM AnalogousDeduped
    UNION ALL
        SELECT
          PolicyRef,
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          GrossNetWrittenPremiumNoRARC,
          GrossLoss,
          GrossLoss / GrossNetWrittenPremiumNoRARC AS PercentageLoss,
          "Exact Ki Match" AS EstimateTag
        FROM KiRenewalMatched
    UNION ALL
        SELECT
          PolicyRef,
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          GrossNetWrittenPremiumNoRARC,
          GrossLoss,
          GrossLoss / GrossNetWrittenPremiumNoRARC AS PercentageLoss,
          "Fuzzy Ki Match" AS EstimateTag
        FROM KiRenewalFuzzyMatched
    UNION ALL
        SELECT
          PolicyRef,
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          GrossNetWrittenPremiumNoRARC,
          GrossLoss,
          GrossLoss / GrossNetWrittenPremiumNoRARC AS PercentageLoss,
          "Exact Brit Match" AS EstimateTag
        FROM BritRenewalMatched
  ),

  CombinedEstimatedLossAggregate AS (
        SELECT
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          AVG(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC,
          AVG(PercentageLoss) AS EstimatedLossPercentage_Expected,
          AVG(PercentageLoss) * AVG(GrossNetWrittenPremiumNoRARC) AS EstimatedLoss_Expected,
          MAX(PercentageLoss) AS EstimatedLossPercentage_Pessimistic,
          MAX(PercentageLoss) * SUM(GrossNetWrittenPremiumNoRARC) AS EstimatedLoss_Pessimistic
        FROM CombinedEstimatedLossPerPolicy
        WHERE Year IS NOT NULL
        GROUP BY
            CaliforniaExposed,
            FloridaExposed,
            GroupClass,
            Event,
            Year
  ),

  Results AS (
    SELECT
      Year,
      Event,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      GrossNetWrittenPremiumNoRARC,
      EstimatedLoss_Expected,
      EstimatedLossPercentage_Expected * 100 AS PercLoss,
      ROW_NUMBER() OVER (ORDER BY EstimatedLoss_Expected) AS SortNum
    FROM CombinedEstimatedLossAggregate
  ),

  TrueLossesPremiumByGroupClass AS (
    SELECT
      GroupClass,
      Event,
      FloridaExposed,
      CaliforniaExposed,
      AVG(GrossNetWrittenPremiumExRARC_USD) AS GrossPremium
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    GROUP BY
      GroupClass,
      Event,
      FloridaExposed,
      CaliforniaExposed
  ),

  TrueLossesPremiumByGroupClassNoEvt AS (
    SELECT
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      AVG(GrossNetWrittenPremiumExRARC_USD) AS GrossPremium
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    GROUP BY
      GroupClass,
      FloridaExposed,
      CaliforniaExposed
  ),

  TrueLosses AS (
    SELECT
      Year,
      GroupClass,
      Event,
      FloridaExposed,
      CaliforniaExposed,
      AVG(tlp.GrossPremium) AS GrossPremium,
      SUM(GrossLoss) AS GrossLoss,
      100 * (SUM(GrossLoss) / AVG(tlp.GrossPremium)) AS GrossLossPercentage
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    JOIN TrueLossesPremiumByGroupClass tlp
    USING (GroupClass, Event, FloridaExposed, CaliforniaExposed)
    GROUP BY
      Year,
      GroupClass,
      Event,
      FloridaExposed,
      CaliforniaExposed
  ),

  TrueLossesNoEvt AS (
    SELECT
      Year,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      AVG(tlp.GrossPremium) AS GrossPremium,
      SUM(GrossLoss) AS GrossLoss,
      100 * (SUM(GrossLoss) / AVG(tlp.GrossPremium)) AS GrossLossPercentage
    FROM AirLossJoinPoliciesPolicyViewAndQuotesFromCached
    JOIN TrueLossesPremiumByGroupClassNoEvt tlp
    USING (GroupClass, FloridaExposed, CaliforniaExposed)
    GROUP BY
      Year,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed
  ),

  NewRollup AS (
    SELECT *
    FROM `ki-datalake-nonprod-a7e5.sandbox.rollup_data_202210_S1618_BN_OM`
  ),

  NewRollupAgg AS (
    SELECT
        Year,
        SUM(GrossLoss) AS GrossLoss
    FROM NewRollup
    GROUP BY Year
  ),

  NewRollupAggRanked AS (
    SELECT
        Year,
        GrossLoss,
        ROW_NUMBER() OVER (ORDER BY GrossLoss DESC) AS SortIdx
    FROM NewRollupAgg
  ),

  EstimationAgg AS (
    SELECT
      Year,
      SUM(GrossNetWrittenPremiumNoRARC) AS GrossNetWrittenPremiumNoRARC,
      SUM(EstimatedLoss_Expected) AS EstimatedLoss_Expected,
      AVG(EstimatedLossPercentage_Expected * 100) AS PercLoss
    FROM CombinedEstimatedLossAggregate
    GROUP BY Year
  ),

  EstimationAggRanked AS (
    SELECT
      Year,
      GrossNetWrittenPremiumNoRARC AS GrossNetWrittenPremiumNoRARC,
      EstimatedLoss_Expected AS EstimatedLoss_Expected,
      PercLoss,
      ROW_NUMBER() OVER (ORDER BY EstimatedLoss_Expected DESC) AS SortIdx
    FROM EstimationAgg
  ),

  CompareWithReal AS (
        SELECT
          Year,
          Event,
          GroupClass,
          FloridaExposed,
          CaliforniaExposed,
          GrossNetWrittenPremiumNoRARC AS EstimatedGNWP,
          TrueLosses.GrossPremium AS TrueGNWP,
          EstimatedLoss_Expected AS EstimatedLoss,
          TrueLosses.GrossLoss AS TrueLoss,
          PercLoss AS EstimatedLossPerc,
          TrueLosses.GrossLossPercentage AS TrueLossPerc

    FROM Results
    JOIN TrueLosses
    USING (Year, Event, GroupClass, FloridaExposed, CaliforniaExposed)
    WHERE PercLoss IS NOT NULL
  ),

  CompareWithRealAnnual AS (
    SELECT
      Year,
      GroupClass,
      FloridaExposed,
      CaliforniaExposed,
      GrossNetWrittenPremiumNoRARC AS EstimatedGNWP,
      TrueLossesNoEvt.GrossPremium AS TrueGNWP,
      EstimatedLoss_Expected AS EstimatedLoss,
      TrueLossesNoEvt.GrossLoss AS TrueLoss,
      PercLoss AS EstimatedLossPerc,
      TrueLossesNoEvt.GrossLossPercentage AS TrueLossPerc

    FROM Results
    JOIN TrueLossesNoEvt
    USING (Year, GroupClass, FloridaExposed, CaliforniaExposed)
    WHERE PercLoss IS NOT NULL
  ),

  TestAggregateEstimationError AS (
    SELECT
        AVG((TrueLoss - EstimatedLoss)) AS ME_LossAbsolute,
        AVG(ABS(TrueLoss - EstimatedLoss)) AS MAE_LossAbsolute,
        AVG(SAFE_DIVIDE(ABS(TrueLoss - EstimatedLoss), TrueLoss)) AS MAPE_LossAbsolute,
        AVG((TrueLossPerc - EstimatedLossPerc)) AS ME_LossPercentage,
        AVG(ABS(TrueLossPerc - EstimatedLossPerc)) AS MAE_LossPercentage,
        SQRT(AVG(POW((EstimatedLossPerc - TrueLossPerc), 2))) AS RMSE_LossPercentage
    FROM CompareWithReal
  )

  TestAggregateEstimationErrorNoEvt AS (
    SELECT
        AVG((TrueLoss - EstimatedLoss)) AS ME_LossAbsolute,
        AVG(ABS(TrueLoss - EstimatedLoss)) AS MAE_LossAbsolute,
        AVG(SAFE_DIVIDE(ABS(TrueLoss - EstimatedLoss), TrueLoss)) AS MAPE_LossAbsolute,
        AVG((TrueLossPerc - EstimatedLossPerc)) AS ME_LossPercentage,
        AVG(ABS(TrueLossPerc - EstimatedLossPerc)) AS MAE_LossPercentage,
        SQRT(AVG(POW((EstimatedLossPerc - TrueLossPerc), 2))) AS RMSE_LossPercentage
    FROM CompareWithRealAnnual
  )
  
{actual_query}
"""
# ===========  WARNING!  ==========
# DO NOT -EVER- USE actual_query OR ANY OTHER FORMAT-STRINGS TO PASS IN QUERY PARAMETERS!
# This is *profoundly* unsafe and begging for getting SQL-injected.
# Use the bindparams API of the query library instead!


@functools.lru_cache(maxsize=1)
def results_query():
    """Returns the primary outputs - predicted loss curves for all GroupClasses/Events/etc."""
    return OEP_CTE_LIBRARY.format(actual_query="SELECT * FROM Results")


@functools.lru_cache(maxsize=1)
def health_metrics_query():
    """Returns 'health checks' for monitoring the quality of the results (MAE/MAPE/RMSE and such)"""
    return OEP_CTE_LIBRARY.format(actual_query="SELECT * FROM TestAggregateEstimationError")


# Begin Prefect workflow code:

@flow()
def run_oep_query():
    gcp_credentials = GcpCredentials.load("BLOCK-NAME-PLACEHOLDER")
    query_params = {}

    with BigQueryWarehouse(gcp_credentials=gcp_credentials) as warehouse:
        warehouse.execute(
            results_query(),
            parameters=query_params
        )

    return
