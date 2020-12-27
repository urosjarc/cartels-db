from src import db, analysis_core, analysis_stock, analysis_anual, analysis_core_anual, analysis_A1012M, analysis_market_indices

db.init_core()
db.init_nodes_stock_meta()
db.init_nodes_annual('/shranjeni/', saved_one=True)
A1012M_type = db.init_nodes_A1012M()
db.init_nodes_market_indices()

"""
core_out_tickers.csv
"""
# analysis_core.EC_duration()
# analysis_core.EC_decision_year()
# analysis_core.EC_decision_May_2004()
# analysis_core.N_firms_within_EC_case()
# analysis_core.N_firms_within_under()
# analysis_core.Multiple_firm_under()
# analysis_core.Repeat_firm_N_EC_cases()
# analysis_core.Recidivist_firm_D()
# analysis_core.Recidivist_firm_2nd_time_D()
# analysis_core.N_Firm_Inc_states_within_EC_case()
# analysis_core.European_firm()
# analysis_core.Extra_Europe_firm()
# analysis_core.EU_all_time_firm()
# analysis_core.EU_EC_decision_firm()
# analysis_core.Old_EU_firm()
# analysis_core.USA_firm()
# analysis_core.Japan_firm()
# analysis_core.Common_Law_Firm()
# analysis_core.English_Law_Firm()
# analysis_core.French_Law_Firm()
# analysis_core.German_Law_Firm()
# analysis_core.Scandinavian_Law_Firm()
# analysis_core.Transcontinental_case()
# analysis_core.National_only_case()
# analysis_core.N_undertaking_within_EC_case()
# analysis_core.Repeat_undertaking_N_EC_cases()
# analysis_core.Recidivist_undertaking_D()
# analysis_core.Recidivist_undertaking_2nd_time_D()
# analysis_core.N_Undertaking_Inc_states_within_EC_case()
# analysis_core.European_undertaking()
# analysis_core.Extra_Europe_undertaking()
# analysis_core.EU_all_time_undertaking()
# analysis_core.EU_EC_decision_undertaking()
# analysis_core.Old_EU_undertaking()
# analysis_core.USA_undertaking()
# analysis_core.Japan_undertaking()
# analysis_core.Common_Law_Undertaking()
# analysis_core.English_Law_Undertaking()
# analysis_core.French_Law_Undertaking()
# analysis_core.German_Law_Undertaking()
# analysis_core.Scandinavian_Law_Undertaking()
# analysis_core.Holding_Ticker_D_Firm()
# analysis_core.Holding_Ticker_D_Undertaking()
# analysis_core.Private_firm()
# analysis_core.Public_firm()
# analysis_core.Association_firm()
# analysis_core.Firm_governance()
# analysis_core.Private_undertaking()
# analysis_core.Public_undertaking()
# analysis_core.Association_undertaking()
# analysis_core.Undertaking_governance()
#
# analysis_core.Case_A101_only()
# analysis_core.Case_A102_only()
# analysis_core.Case_A101_102_only()
# analysis_core.Case_a101()
# analysis_core.Case_a102()
# analysis_core.Case_cartel_VerR()
# analysis_core.Case_Ringleader()
#
# analysis_core.Case_A101_only_undertaking()
# analysis_core.Case_A102_only_undertaking()
# analysis_core.Case_A101_102_only_undertaking()
# analysis_core.Case_a101_undertaking()
# analysis_core.Case_a102_undertaking()
# analysis_core.Case_cartel_VerR_undertaking()
# analysis_core.Case_Ringleader_undertaking()
#
analysis_core.Investigation_begin()
# analysis_core.Investigation_begin_year()
analysis_core.Investigation_begin_without_dawn_raid()
# analysis_core.Dawn_raid_year()
# analysis_core.InfringeDurationOverallFirm()
# analysis_core.InfringeDurationOverallUndertaking()
# analysis_core.Ticker_firm_D()
# analysis_core.Ticker_case_D()
# analysis_core.Ticker_undertaking_D()
analysis_core.Infringement_begin()
# analysis_core.InfringeDurationOverallCase()
# analysis_core.In_flagrante_investigation_firm()
# analysis_core.In_flagrante_investigation_undertaking()
# analysis_core.In_flagrante_investigation_case()
# analysis_core.InfringeEndByDecision_firm()
# analysis_core.InfringeEndByDecision_undertaking()
# analysis_core.InfringeEndByDecision_case()
# analysis_core.InfringeDurationTillInvestigationFirm()
# analysis_core.InfringeDurationTillInvestigationUndertaking()
# analysis_core.InfringeDurationTillInvestigationCase()
# analysis_core.Infringements_per_firm()
# analysis_core.Infringements_per_undertaking()
# analysis_core.Infringements_per_case()
# analysis_core.TwoOrMoreInfringements_per_firm()
# analysis_core.TwoOrMoreInfringements_per_undertaking()
# analysis_core.TwoOrMoreInfringements_per_case()
# analysis_core.MaxIndividualInfringeDurationFirm()
# analysis_core.MaxIndividualInfringeDurationUndertaking()
# analysis_core.MaxIndividualInfringeDurationCase()
# analysis_core.InfringeBeginYearFirm()
# analysis_core.InfringeBeginYearUndertaking()
# analysis_core.InfringeBeginYearCase()
# analysis_core.Settlement_fine_firm()
# analysis_core.Settlement_fine_undertaking()
# analysis_core.Settlement_fine_case()
# analysis_core.Full_immunity_firm()
# analysis_core.Full_immunity_undertaking()
# analysis_core.Full_immunity_case()
# analysis_core.Fine_imposed_D_firm()
# analysis_core.Fine_imposed_D_undertaking()
# analysis_core.Fine_imposed_D_case()
# analysis_core.Fine_max_firm()
# analysis_core.Fine_firm()
# analysis_core.Fine_undertaking()
# analysis_core.Fine_case()
# analysis_core.GC_fine_change_D_firm()
# analysis_core.GC_fine_change_D_undertaking()
# analysis_core.GC_fine_change_D_case()
# analysis_core.ECJ_fine_change_D_firm()
# analysis_core.ECJ_fine_change_D_undertaking()
# analysis_core.ECJ_fine_change_D_case()
# analysis_core.GC_fine_change_firm()
# analysis_core.ECJ_fine_change_firm()
# analysis_core.ECJ_fine_change_undertaking()
# analysis_core.ECJ_fine_change_case()
# analysis_core.ECJ_fine_percent_reduction_firm()
# analysis_core.ECJ_fine_percent_reduction_undertaking()
# analysis_core.ECJ_fine_percent_reduction_case()
# analysis_core.ECJ_fine_relative_percent_reduction_case()
# analysis_core.GC_fine_change_undertaking()
# analysis_core.GC_fine_change_case()
# analysis_core.GC_fine_percent_reduction_firm()
# analysis_core.GC_fine_percent_reduction_undertaking()
# analysis_core.GC_fine_percent_reduction_case()
# analysis_core.GC_fine_relative_percent_reduction_case()
# analysis_core.LeniencyFineReduction_D_firm()
# analysis_core.LeniencyFineReduction_D_undertaking()
# analysis_core.LeniencyFineReduction_D_case()
# analysis_core.LeniencyPercentMaxRed_firm()
# analysis_core.LeniencyPercentMaxRed_undertaking()
# analysis_core.LeniencyPercentMaxRed_case()
# analysis_core.LeniencyPercentMinRed_firm()
# analysis_core.LeniencyPercentMinRed_undertaking()
# analysis_core.LeniencyPercentMinRed_case()
# analysis_core.LeniencyPercentAvgRed_firm()
# analysis_core.LeniencyPercentAvgRed_undertaking()
# analysis_core.LeniencyPercentAvgRed_case()
# analysis_core.DUMIES_10()
# analysis_core.Type_of_investigation_begin()
# analysis_core.RecitalsEC_per_firm()
# analysis_core.RecitalsEC_per_undertaking()
# analysis_core.Articles_of_remedy_per_firm()
# analysis_core.Articles_of_remedy_per_undertaking()
# analysis_core.Market_of_concern_COLUMS()
# analysis_core.Settlement_Whistleblower_Leniency_application_Dawn_raid_F_specific()
# analysis_core.Dawn_raid_USA_Japan_D()
# analysis_core.Extra_EU_ongoing_invest_D()
# analysis_core.GC_decision_year()
# analysis_core.GC_columns()
# analysis_core.ECJ_decision_year()
# analysis_core.ECJ_columns()
# analysis_core.GC_N_judgements_on_EC_case_per_undertaking()
# analysis_core.GC_N_judgements_on_EC_case()
# analysis_core.GC_Filing_action_D_firm()
# analysis_core.GC_Filing_action_D_undertaking()
# analysis_core.GC_Filing_action_D_case()
# analysis_core.GC_N_firm_plaintiffs_EC_case()
# analysis_core.GC_N_undertaking_plaintiffs_EC_case()
# analysis_core.GC_EC_ratio_for_N_firm_plaintiffs_EC_case()
# analysis_core.GC_EC_ratio_for_N_undertaking_plaintifs_EC_case()
# analysis_core.GC_judgement_N_plaintiffs_firm()
# analysis_core.GC_judgement_N_plaintiffs_undertaking()
# analysis_core.GC_duration_firm()
# analysis_core.GC_duration_undertaking()
# analysis_core.GC_duration_case()
# analysis_core.GC_Pending()
# analysis_core.GC_Pending_undertaking()
# analysis_core.GC_Pending_case()
# analysis_core.GC_total_loss_firm()
# analysis_core.GC_total_success_firm()
# analysis_core.GC_partial_success_firm()
# analysis_core.GC_partial_success_1()
# analysis_core.GC_total_loss_undertaking()
# analysis_core.GC_total_loss_case()
# analysis_core.GC_total_success_undertaking()
# analysis_core.GC_total_success_case()
# analysis_core.GC_partial_success_undertaking()
# analysis_core.GC_partial_success_case()
# analysis_core.GC_judgement_paragraphs_per_firm()
# analysis_core.GC_judgement_paragraphs_per_undertaking()
# analysis_core.GC_judgement_paragraphs_for_EC_case()
# analysis_core.GC_judgement_Articles_per_firm()
# analysis_core.GC_judgement_Articles_per_undertaking()
# analysis_core.GC_judgement_Articles_for_EC_case()
# analysis_core.ECJ_N_judgements_on_EC_case_per_undertaking()
# analysis_core.ECJ_N_judgements_on_EC_case()
# analysis_core.ECJ_Appeal_lodged_D_firm()
# analysis_core.ECJ_Appeal_lodged_D_undertaking()
# analysis_core.ECJ_Appeal_lodged_D_case()
# analysis_core.ECJ_N_firm_appellants_EC_case()
# analysis_core.ECJ_N_undertaking_appellants_EC_case()
# analysis_core.ECJ_EC_ratio_for_N_firm_appellants_EC_case()
# analysis_core.ECJ_EC_ratio_for_N_undertaking_plaintifs_EC_case()
# analysis_core.ECJ_judgement_N_appellants_firm()
# analysis_core.ECJ_judgement_N_appellants_undertaking()
# analysis_core.ECJ_duration_firm()
# analysis_core.ECJ_duration_undertaking()
# analysis_core.ECJ_duration_case()
# analysis_core.ECJ_Pending()
# analysis_core.ECJ_Pending_case()
# analysis_core.ECJ_Pending_undertaking()
# analysis_core.ECJ_total_loss_firm()
# analysis_core.ECJ_total_success_firm()
# analysis_core.ECJ_partial_success_firm()
# analysis_core.ECJ_total_loss_undertaking()
# analysis_core.ECJ_total_loss_case()
# analysis_core.ECJ_total_success_undertaking()
# analysis_core.ECJ_total_success_case()
# analysis_core.ECJ_partial_success_undertaking()
# analysis_core.ECJ_partial_success_case()
# analysis_core.ECJ_judgement_paragraphs_per_firm()
# analysis_core.ECJ_judgement_paragraphs_per_undertaking()
# analysis_core.ECJ_judgement_paragraphs_for_EC_case()
# analysis_core.ECJ_judgement_Articles_per_firm()
# analysis_core.ECJ_judgement_Articles_per_undertaking()
# analysis_core.ECJ_judgement_Articles_for_EC_case()
# analysis_core.NEWS_EVENT_FILE()
# analysis_core.EVENT_FILES()
# analysis_core.Commissioner_for_competition_investigation_begin()
# analysis_core.Commissioner_for_competition_EC_decision()
# analysis_core.Commission_President_investigation_begin()
# analysis_core.Commission_President_EC_decision()
# analysis_core.Commission_caseload_Investigation_begin()
# analysis_core.Commission_caseload_EC_decision()
# analysis_core.ECJ_pending_cases_EC_decision()
# analysis_core.GC_pending_cases_EC_decision()
# analysis_core.EPP_D_Investigation_begin()
# analysis_core.EPP_D_EC_decision()
# analysis_core.Leniency_notice_1996_D_Investigation_begin()
# analysis_core.Leniency_notice_2002_D_Investigation_begin()
# analysis_core.Leniency_notice_2006_D_Investigation_begin()
# analysis_core.Leniency_notice_1996_D_EC_decision()
# analysis_core.Leniency_notice_2002_D_EC_decision()
# analysis_core.Leniency_notice_2006_D_EC_decision()
# analysis_core.Investigation_begin_May_2004()
# analysis_core.Fining_guidelines_1998_D_Investigation_begin()
# analysis_core.Fining_guidelines_2006_D_Investigation_begin()
# analysis_core.Fining_guidelines_1998_D_EC_decision()
# analysis_core.Fining_guidelines_2006_D_EC_decision()
# analysis_core.Settlement_regulation_D_Investigation_begin()
# analysis_core.Settlement_regulation_D_EC_decision()
#
# analysis_stock.Active_ticker_D()
# analysis_stock.Euro_currency_ticker_D()
# analysis_stock.Active_date()
# analysis_stock.Stock_exchange_name()
# analysis_stock.Multiple_listings_D()
# analysis_stock.Stock_indexing_D()
# analysis_stock.Multishare_Corporation_D()
# analysis_stock.ADR_D()
# analysis_stock.Board_Structure_Type()
# analysis_stock.Product_market()
# analysis_stock.Level_2_sector_name()
# analysis_stock.Level_3_sector_name()
# analysis_stock.Level_4_sector_name()
# analysis_stock.Level_5_sector_name()
# analysis_stock.Local_index()
# analysis_stock.Datastream_local_index()
# analysis_stock.ICB_industry_DS_index()
# analysis_stock.FT_sector_DS_index()
#
# """
# annual_[eu/local]_out_tickers.csv
# """
# analysis_anual.Current_ratio()
# analysis_anual.Acid_test_ratio()
# analysis_anual.Cash_ratio()
# analysis_anual.Debt_ratio()
# analysis_anual.Debt_to_equity_ratio()
# analysis_anual.Equity_ratio()
#
# '''
# Reloading with new rows
# '''
# db.save_annual()
# db.init_nodes_annual('/../', saved_one=False)
#
# """
# annual connection to core
# """
#
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearFirm')
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearFirm', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearFirm', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearFirm', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearFirm', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearFirm', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearFirm', trend_year=4)
#
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', ticker='Ticker_undertaking')
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', ticker='Ticker_undertaking', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', ticker='Ticker_undertaking', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', ticker='Ticker_undertaking', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=1, ticker='Ticker_undertaking')
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=3, ticker='Ticker_undertaking')
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=5, ticker='Ticker_undertaking')
#
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', info='HOLDING', ticker='Holding_Ticker_parent')
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', info='HOLDING', ticker='Holding_Ticker_parent', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', info='HOLDING', ticker='Holding_Ticker_parent', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('InfringeBeginYearUndertaking', info='HOLDING', ticker='Holding_Ticker_parent', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=1, info='HOLDING', ticker='Holding_Ticker_parent')
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=3, info='HOLDING', ticker='Holding_Ticker_parent')
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('InfringeBeginYearUndertaking', trend_year=5, info='HOLDING', ticker='Holding_Ticker_parent')
#
# analysis_core_anual.Ime_Currency_VAR_Info('Investigation_begin_year')
# analysis_core_anual.Ime_Currency_VAR_Info('Investigation_begin_year', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('Investigation_begin_year', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('Investigation_begin_year', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Investigation_begin_year', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Investigation_begin_year', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Investigation_begin_year', trend_year=5)
#
# analysis_core_anual.Ime_Currency_VAR_Info('Dawn_raid_year')
# analysis_core_anual.Ime_Currency_VAR_Info('Dawn_raid_year', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('Dawn_raid_year', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('Dawn_raid_year', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Dawn_raid_year', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Dawn_raid_year', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('Dawn_raid_year', trend_year=5)
#
# analysis_core_anual.Ime_Currency_VAR_Info('EC_decision_year')
# analysis_core_anual.Ime_Currency_VAR_Info('EC_decision_year', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('EC_decision_year', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('EC_decision_year', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('EC_decision_year', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('EC_decision_year', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('EC_decision_year', trend_year=5)
#
# analysis_core_anual.Ime_Currency_VAR_Info('GC_decision_year')
# analysis_core_anual.Ime_Currency_VAR_Info('GC_decision_year', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('GC_decision_year', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('GC_decision_year', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('GC_decision_year', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('GC_decision_year', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('GC_decision_year', trend_year=5)
#
# analysis_core_anual.Ime_Currency_VAR_Info('ECJ_decision_year')
# analysis_core_anual.Ime_Currency_VAR_Info('ECJ_decision_year', year_minus=1)
# analysis_core_anual.Ime_Currency_VAR_Info('ECJ_decision_year', year_minus=3)
# analysis_core_anual.Ime_Currency_VAR_Info('ECJ_decision_year', year_minus=5)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('ECJ_decision_year', trend_year=1)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('ECJ_decision_year', trend_year=3)
# analysis_core_anual.Ime_Currency_VAR_Info_YearTrend('ECJ_decision_year', trend_year=5)
#
# analysis_core_anual.Ime__Currency__Infr_begin_to_Inv_Beg_trend()
# analysis_core_anual.Ime__Currency__Infr_begin_to_EC_Dec_trend()
# analysis_core_anual.Ime__Currency__Inv_begin_to_EC_Dec_trend()
# analysis_core_anual.Ime__Currency__EC_Dec_to_GC_Dec_trend()
# analysis_core_anual.Ime__Currency__GC_Dec_to_ECJ_Dec_trend()
#
# if analysis_A1012M != None:
#     """
#     Analysis A1012M
#     """
#     for name in db.names_A1012M:
#         print(f"Analysis A1012M: {name}")
#         analysis_A1012M.NAMES_A1012(name, A1012M_type)
#
#     '''
#     New wars in A1012M
#     '''
#     analysis_A1012M.momentum_year(A1012M_type)
#     analysis_A1012M.raw_returns(A1012M_type)
#     analysis_A1012M.ln_returns(A1012M_type)
#
#     print("Saving A1012M...")
#     db.save_A1012M(A1012M_type)
#
#
analysis_market_indices.NAMES_DSLOC()
analysis_market_indices.NAMES_LEV2IN()
analysis_market_indices.NAMES_LEV4SE()
analysis_market_indices.NAMES_MLOC()
analysis_market_indices.NAMES_TOTMKWD()

# analysis_market_indices.momentum_year()
# analysis_market_indices.raw_returns()
# analysis_market_indices.ln_returns()


db.save_market_indices()
# db.save_core()
# db.save_annual()
