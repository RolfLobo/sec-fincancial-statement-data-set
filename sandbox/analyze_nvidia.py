from secfsdstools.e_collector.reportcollecting import SingleReportCollector
from secfsdstools.e_filter.rawfiltering import (
    MainCoregRawFilter,
    OfficialTagsOnlyRawFilter,
    ReportPeriodRawFilter,
    USDOnlyRawFilter,
)
from secfsdstools.f_standardize.is_standardize import IncomeStatementStandardizer




def read_data():
    """
        adsh                                                0001045810-25-000023
        cik                                                              1045810
        name                                                         NVIDIA CORP
        form                                                                10-K
        fye                                                                 0131
        fy                                                                2024.0
        fp                                                                    FY
        date                                                 2025-01-31 00:00:00
        filed                                                           20250226
        coreg
        report                                                                 3
        ddate                                                           20250131
        qtrs                                                                   4

        Revenues                                                  130497000000.0
        CostOfRevenue                                              32639000000.0
        GrossProfit                                                97858000000.0
        OperatingExpenses                                          16405000000.0
        OperatingIncomeLoss                                        81453000000.0
        IncomeLossFromContinuingOperationsBeforeIncomeT...         84026000000.0

        AllIncomeTaxExpenseBenefit                                  6669000000.0
        IncomeLossFromContinuingOperations                         77357000000.0
        IncomeLossFromDiscontinuedOperationsNetOfTax                         0.0
        ProfitLoss                                                 77357000000.0
        NetIncomeLossAttributableToNoncontrollingInterest           4477000000.0
        NetIncomeLoss                                              72880000000.0

        OutstandingShares                                          24555000000.0
        EarningsPerShare                                                    2.97
        RevCogGrossCheck_error                                               0.0
        RevCogGrossCheck_cat                                                 0.0
        GrossOpexpOpil_error                                                 0.0
        GrossOpexpOpil_cat                                                   0.0
        ContIncTax_error                                                     0.0
        ContIncTax_cat                                                       0.0
        ProfitLoss_error                                                     0.0
        ProfitLoss_cat                                                       0.0
        NetIncomeLoss_error                                                  0.0
        NetIncomeLoss_cat                                                    0.0
        EPS_error                                                       0.000663
        EPS_cat                                                              1.0    


        Applied Rules
        MAIN_1_IS_#6_OperatingExpenses_#1_Pre_#1_OperatingExpensesSum
        MAIN_1_IS_#7_inclossbeforetax_#2_Set_#1_IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit<-IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest
        MAIN_1_IS_#8_netincome_#1_Pre_#1_AllIncomeTaxExpenseBenefit
        MAIN_1_IS_#8_netincome_#1_Pre_#3_IncomeLossFromContinuingOperations
        MAIN_1_IS_#8_netincome_#1_Pre_#5_ProfitLossParts
        MAIN_1_IS_#8_netincome_#2_SetPL_#1_ProfitLoss<-ProfitLossParts
        MAIN_1_IS_#10_NetIncomeLossAttributableToNoncontrollingInterest
        MAIN_1_IS_#11_sharesAndEPS_#1_OutstandingShares<-WeightedAverageNumberOfSharesOutstandingBasic
        MAIN_1_IS_#11_sharesAndEPS_#8_EarningsPerShare<-EarningsPerShareBasic
        POST_IS_#11_IncomeLossFromDiscontinuedOperationsNetOfTax        
            
        """



    bag = SingleReportCollector.get_report_by_adsh(adsh="0001045810-25-000023").collect()
    filtered_bag = bag[ReportPeriodRawFilter()][MainCoregRawFilter()][OfficialTagsOnlyRawFilter()][USDOnlyRawFilter()]
    joined_bag = filtered_bag.join()

    print("\nRaw Num Data")
    print(filtered_bag.num_df[filtered_bag.num_df.tag.isin(['IncomeTaxExpenseBenefit', 'DeferredIncomeTaxExpenseBenefit'])].head(50))


    print("\nJoined Data")
    pre_num_data_no_segments = joined_bag.pre_num_df[((joined_bag.pre_num_df.segments == '') | joined_bag.pre_num_df.segments.isna()) & (joined_bag.pre_num_df.stmt == 'IS')]
    print(pre_num_data_no_segments[['tag', 'version', 'value', 'line']].sort_values(by=['line']).head(50))

    is_standardizer = IncomeStatementStandardizer()
    standardized_is_df = joined_bag.present(is_standardizer)

    print("\nStandardizer")
    filter_df = standardized_is_df[standardized_is_df.date == "2025-01-31"]
    print("\nStandardized Data")
    print(filter_df.T.head(40))

    is_standardizer_result_bag = is_standardizer.get_standardize_bag()
    applied_rules_log_df = is_standardizer_result_bag.applied_rules_log_df[is_standardizer_result_bag.applied_rules_log_df.adsh=="0001045810-25-000023"]


    # filter for the applied MAIN,PRE, and POST rules
    main_rule_cols =  applied_rules_log_df.columns[applied_rules_log_df.columns.str.contains('MAIN|PRE|POST')]
    main_rule_df = applied_rules_log_df[main_rule_cols]

    # get the applied rules, by using the True and False values of main_rule_df.iloc[0] as a mask on the columns index
    print("\nApplied Rules")
    for entry in main_rule_df.columns[main_rule_df.iloc[0]].tolist():
        print(entry)


if __name__ == "__main__":
    # comment
    # comment 2
    read_data()
