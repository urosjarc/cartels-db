from py2neo import Node as NeoNode


class Path:
    def __init__(self, json: dict):
        self.confidence = -1
        self.street = None
        self.number = None
        self.name = None
        self.type = None
        self.latitude = None
        self.longitude = None

        for attr, val in self.__dict__.items():
            if val is None:
                setattr(self, attr, json.get(attr, None))


class CSVRow:
    def __init__(self, row: dict):
        delKeys = []
        pairs = []
        for k, v in row.items():
            new_key = k.replace('(', '').replace(')', '').replace('/', '_')
            pairs.append([new_key,v])
            delKeys.append(k)
        for k in delKeys:
            row.pop(k)
        for k, v in pairs:
            row[k] = v
        self._row = row

        self.firm = Firm()
        self.case = Case()
        self.undertaking = Undertaking()
        self.holding = Holding()

        self._createNodes()

    def _createNodes(self):
        for attr, val in self.__dict__.items():
            if isinstance(val, Node):
                node = getattr(self, attr)
                for nodeAttr in node.__dict__:
                    if not nodeAttr.startswith('_'):
                        node._setVar(nodeAttr, self._row[nodeAttr])

                node._data = self._row
                node.post_init()
                node._init()


class Node:
    def __init__(self, name):
        self._data = None
        self._exists = None
        self._name = name
        self._instance = None

    def _setVar(self, key, value):
        setattr(self, key, value)

    def __str__(self):
        return str(self._instance)

    def _getAttr(self):
        varg = {}
        for name, val in self.__dict__.items():
            if not name.startswith('_'):
                varg[name] = val
        return varg

    def _init(self):
        name = getattr(self, self._name, False)
        self._exists = name and not name.isspace()

        if self._exists:
            self._instance = NeoNode(self._name, **self._getAttr())

    def post_init(self):
        pass

class Case(Node):
    def __init__(self):
        super().__init__('Case')
        self.NACE_code = None
        self.Case = None
        self.Press_Release = None
        self.EC_Date_of_decision = None
        self.GC_Case_number = None
        self.ECJ_Case_number = None
        self.Infringement = None
        self.Case_File = None
        self.Case_File_summary = None
        self.Case_File_French = None
        self.Case_File_Italian = None
        self.Case_File_German = None
        self.Case_File_Dutch = None

        self.GC_File = None
        self.ECJ_File = None
        self.EC_Event_dec_file = None

        self.ECSC = None

        self.Readoption_amendment = None
        self.Ex_offo = None
        self.Notification = None
        self.Notification_additional_to_complaint = None
        self.Complaint = None
        self.Complaint_post_initiation = None
        self.Leniency = None
        self.Dawn_raid = None
        self.Statement_of_objections = None
        self.Recitals = None
        self.Articles_of_remedy = None
        self.Market_of_concern = None
        self.Industry = None
        self.Dawn_raid_USA_Japan = None
        self.Extra_EU_ongoing_invest = None
        self.EC_pre_dec_event = None
        self.EC_dec_event = None
        self.M20_DR_date = None
        self.M20_EC_date = None
        self.M20ticker = None

        self.EC_Event_dec_file = None


class Firm(Node):
    def __init__(self):
        super().__init__('Firm')
        self.Firm = None
        self.Incorporation_state = None
        self.Firm_address = None
        self.Firm_type = None
        self.Ticker_firm = None
        self.Stock_exchange_firm = None
        self.InfrBegin1 = None
        self.InfrEnd1 = None
        self.InfrBegin2 = None
        self.InfrEnd2 = None
        self.InfrBegin3 = None
        self.InfrEnd3 = None
        self.InfrBegin4 = None
        self.InfrEnd4 = None
        self.InfrBegin5 = None
        self.InfrEnd5 = None
        self.InfrBegin6 = None
        self.InfrEnd6 = None
        self.InfrBegin7 = None
        self.InfrEnd7 = None
        self.InfrBegin8 = None
        self.InfrEnd8 = None
        self.InfrBegin9 = None
        self.InfrEnd9 = None
        self.InfrBegin10 = None
        self.InfrEnd10 = None
        self.InfrBegin11 = None
        self.InfrEnd11 = None
        self.InfrBegin12 = None
        self.InfrEnd12 = None
        self.Fine_reduction_due_to_cooperation_in_percentage = None
        self.Settlement_fine_reduction_in_percentage = None
        self.Full_immunity = None
        self.Fine_final_single_firm = None
        self.Leniency__Single_Fine_red_in_percent = None
        self.GC_case_SF = None
        self.ECJ_SF_fine = None
        self.Fine_jointly_severally_1 = None
        self.Reduction_1 = None
        self.GC_case_JSF1 = None
        self.ECJ_JSF1 = None
        self.Fine_jointly_severally_2 = None
        self.Reduction_2 = None
        self.GC_case_JSF2 = None
        self.ECJ_JSF2 = None
        self.Fine_jointly_severally_3 = None
        self.Reduction_3 = None
        self.GC_case_JSF3 = None
        self.ECJ_JSF3 = None
        self.Fine_jointly_severally_4 = None
        self.Reduction_4 = None
        self.GC_case_JSF4 = None
        self.Fine_jointly_severally_5 = None
        self.Reduction_5 = None
        self.GC_case_JSF5 = None
        self.Fine_jointly_severally_6 = None
        self.Reduction_6 = None
        self.GC_case_JSF6 = None
        self.Fine_jointly_severally_7 = None
        self.Reduction_7 = None
        self.GC_case_JSF7 = None
        self.Structural_remedy = None
        self.Behavioral_remedy = None
        self.Concrete_Behavioral_Remedy = None
        self.Settlement = None
        self.Whistleblower = None
        self.Leniency_application = None
        self.Dawn_raid_F_specific = None
        self.Other_decisions_procedures = None
        self.GC_File = None
        self.GC_File_summary = None
        self.GC_File_French = None
        self.GC_File_Italian = None
        self.GC_File_German = None
        self.GC_File_Spanish = None
        self.GC_File_Dutch = None
        self.Press_release = None
        self.GC_New_party = None
        self.GC_new_party_state = None
        self.GC_Case_number = None
        self.GC_Filing_action = None
        self.GC_Decision_date = None
        self.GC_Judgement_order = None
        self.GC_Chamber_of_3 = None
        self.GC_Chamber_of_5 = None
        self.GC_Grand_Chamber = None
        self.GC_Dismissing_action__entirely = None
        self.GC_Manifestly_inadmissible = None
        self.GC_Inadmissible = None
        self.GC_Total_action_success = None
        self.GC_No_need_to_adjudicate = None
        self.GC_Total_annulment_of_EC_decision = None
        self.GC_Partial_annulment_of_EC_decision = None
        self.GC_Fine_partial_change_of_EC_decision = None
        self.GC_Change_of_other_remedies_of_EC_decision = None
        self.GC_judgement_Summary = None
        self.GC_judgment_Paragraphs = None
        self.GC_judgement_Articles = None
        self.ECJ_File = None
        self.ECJ_File_summary = None
        self.ECJ_File_French = None
        self.ECJ_File_Italian = None
        self.ECJ_File_German = None
        self.ECJ_File_Spanish = None
        self.Press_release = None
        self.ECJ_New_party = None
        self.ECJ_new_party_State = None
        self.ECJ_Commission_appeal = None
        self.ECJ_Cross_appeal_of_Commission = None
        self.ECJ_Case_number = None
        self.ECJ_Appeal_lodged = None
        self.ECJ_Decision_date = None
        self.ECJ_Judgement_order = None
        self.ECJ_Chamber_of_3 = None
        self.ECJ_Chamber_of_5 = None
        self.ECJ_Grand_Chamber = None
        self.AG_opinion = None
        self.ECJ_Dissmissing_appeal = None
        self.ECJ_Comm_T_Dissmiss = None
        self.ECJ_Comm_P_Dissmiss = None
        self.ECJ_Comm_T_Grant = None
        self.ECJ_total_referral = None
        self.ECJ_Partial_referral = None
        self.ECJ_Total_change_of_GC_judgement = None
        self.ECJ_Total_party_appeal_success = None
        self.ECJ_Partial_change_of_EC_decision = None
        self.ECJ_Total_confirmation_of_EC_decision_dissmisal_of_action_on_1st_instance = None
        self.ECJ_Total_annulment_of_EC_decision = None
        self.ECJ_Partial_annulment_of_EC_decision = None
        self.ECJ_Fine_partial_change_of_EC_decision = None
        self.ECJ_Change_of_other_remedies_of_EC_decision = None
        self.ECJ_judgement_Summary = None
        self.ECJ_judgement_Paragraphs = None
        self.ECJ_judgement_Articles = None
        self.DR_dec_2M = None
        self.DR_dec_15d = None
        self.DR_dec_15d_DJN = None
        self.DR_dec_15d_R = None
        self.DR_dec_15d_FT = None
        self.DR_dec_15d_WSJ = None
        self.GC_pre_dec_event = None
        self.GC_dec_event = None
        self.GC_dec_2M = None
        self.GC_dec_15d = None
        self.GC_dec_15d_DJN = None
        self.GC_dec_15d_R = None
        self.GC_dec_15d_FT = None
        self.GC_dec_15d_WSJ = None

        self.A_101 = None
        self.A_102 = None
        self.A101_102 = None
        self.a_101 = None
        self.a_102 = None
        self.Cartel_VerR = None
        self.Ringleader = None

        self.DR_Event_File = None
        self.DR_Date_News = None
        self.DR_dec_event = None

        self.GC_Event_File = None
        self.ECJ_Event_File = None

        self.ECJ_pre_dec_event = None
        self.ECJ_dec_event = None
        self.ECJ_dec_2M = None
        self.ECJ_dec_15d = None
        self.ECJ_dec_15d_DJN = None
        self.ECJ_dec_15d_R = None
        self.ECJ_dec_15d_FT = None
        self.ECJ_dec_15d_WSJ = None

    def post_init(self):
            type = None
            if 'association' in self.Firm_type:
                type = 'association'
            elif self.Ticker_firm is None:
                type = 'private'
            else:
                type = 'public'
            self.Ticker_firm = type

class Undertaking(Node):
    def __init__(self):
        super().__init__('Undertaking')
        self.Undertaking = None
        self.IncorpStateUnder = None
        self.IncorpStateUnderJS = None
        self.Under_address = None
        self.Ticker_undertaking = None
        self.Stock_exchange_undertaking = None
        self._Undertaking_type = None

    def post_init(self):
        type = None

        if self.Undertaking == self._data['Firm']:
            if 'association' in self.Ticker_undertaking:
                type = 'association'
            elif self.Ticker_undertaking is None:
                type = 'private'
            else:
                type = 'public'

        self._Undertaking_type = type


class Holding(Node):
    def __init__(self):
        super().__init__('Holding')
        self.Holding = None
        self.Holding_Ticker_parent = None
        self.Stock_exchange_holding = None
        self.IncorpStateHold = None


# Todo:
#   - Undertaking_type
# OPOMBA(pass)  - Pecularities_of_undertaking_M&A_or_restructuring
# OK(firm)  - A_101
# OK(firm)  - A_102
# OK(firm)  - A101_102
# OK(firm)  - a_101
# OK(firm)  - a_102
# OK(firm)  - Cartel_VerR
# OK(firm)  - Ringleader
# IGNORE(pass)  - CJE_Appeal
# IGNORE(pass)  - CJE_Case_number_for_referral
# IGNORE(pass)  - GC_referral_New_party
# IGNORE(pass)  - GC_referral_new_party_state
# IGNORE(pass)  - GC_referral_Case_number
# IGNORE(pass)  - GC_referral_Filing_action
# IGNORE(pass)  - GC_referral_Decision_date
# IGNORE(pass)  - GC_referral_Judgement/order
# IGNORE(pass)  - GC_referral_Chamber_of_3
# IGNORE(pass)  - GC_referral_Chamber_of_5
# IGNORE(pass)  - GC_referral_Grand_Chamber
# IGNORE(pass)  - GC_referral_Dismissing_action__entirely
# IGNORE(pass)  - GC_referral_Manifestly_inadmissible
# IGNORE(pass)  - GC_referral_Inadmissible
# IGNORE(pass)  - GC_referral_Total_action_success
# IGNORE(pass)  - GC_referral_No_need_to_adjudicate
# IGNORE(pass)  - GC_referral_Total_annulment_of_EC_decision
# IGNORE(pass)  - GC_referral_Partial_annulment_of_EC_decision
# IGNORE(pass)  - GC_referral_Fine_partial_change_of_EC_decision
# IGNORE(pass)  - GC_referral_Change_of_other_remedies_of_EC_decision
# IGNORE(pass)  - GC_referral_judgment_Paragraphs
# IGNORE(pass)  - GC_referral_judgement_Articles
# IGNORE(pass)  - GC_referral_judgement_Summary
# OK(case)  - EC_Event_dec_file
# MOVE(firm) - DR_Event_File
# MOVE(case->firm) - DR_Date_News
# MOVE(case->firm) - DR_dec_event
# OK(firm)  - GC_Event_File
# OK(firm)  - CJ_Event_File
# RENAME(CJ...->ECJ...)  - CJ_pre_dec_event
# RENAME(CJ...->ECJ...)  - CJ_dec_event
# RENAME(CJ...->ECJ...)  - CJ_dec_2M
# RENAME(CJ...->ECJ...)  - CJ_dec_15d
# RENAME(CJ...->ECJ...)  - CJ_dec_15d_DJN
# RENAME(CJ...->ECJ...)  - CJ_dec_15d_R
# RENAME(CJ...->ECJ...)  - CJ_dec_15d_FT
# RENAME(CJ...->ECJ...)  - CJ_dec_15d_WSJ
