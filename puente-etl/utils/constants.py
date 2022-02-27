class ColumnOrder:
    # Enforce column order for relational table outputs
    FORM_SPECIFICATIONS = [
        'custom_form_id',
        'custom_form_organizations',
        'custom_form_class',
        'custom_form_type_of_form',
        'custom_form_name',
        'custom_form_description',
        'custom_form_header',
        #
        # Excluding these for now for readability:
        #
        # 'custom_form_updated_at',
        # 'custom_form_created_at',
        # 'custom_form_custom_form',
        # 'custom_form_workflows',
        # 'custom_form_active',
        'question_id',
        'question_label',
        'question_text',
        'question_field_type',
        'question_formik_key',
        'question_value',
        #
        # Excluding these for now for readability:
        #
        # 'question_active',
        # 'question_number_questions_to_repeat',
        # 'question_side_label',
        # 'question_validation',
        'answer_id',
        'answer_label',
        'answer_value',
        'answer_text',
        'answer_text_key',
        'answer_text_question'
    ]

    FORM_RESULTS = [
        'form_result_id',
        'form_result_surveying_organization',
        'form_result_surveying_user',
        'custom_form_id',
        'form_result_response_id',
        'question_title',
        'question_answer',
        'question_type',
        #
        # Excluding these for now for readability:
        #
        # 'custom_form_name',
        # 'custom_form_description',
        # 'form_result_created_at',
        # 'form_result_updated_at',
        # 'form_result_p_client',
        # 'form_result_p_parse_user',
    ]


class ColumnReplace:
    FORM_RESULTS = {
        'id': 'form_result_id',
        'form_specifications_id': 'form_result_custom_form_id',
        'fields': 'form_result_fields',
        'p_client': 'form_result_p_client',
        'p_parse_user': 'form_result_p_parse_user',
        'created_at': 'form_result_created_at',
        'updated_at': 'form_result_updated_at',
        'description': 'form_result_question_description',
        'organizations': 'custom_form_fields',
        'surveying_organization': 'form_result_surveying_organization',
        'surveying_user': 'form_result_surveying_user',
        'title': 'custom_form_header'
    }


class Outputs:
    JSON = 'json'
    PICKLE_DICT = 'pickle_dict'
    PICKLE_DATAFRAME = 'pickle_df'
    PICKLE_LIST = 'pickle_list'


class PuenteTables:
    ROLE = 'Role'
    SESSION = 'Session'
    USER = 'User'
    ALLERGIES = 'Allergies'
    ASSETS = 'Assets'
    # B4A_CUSTOM_FIELD = 'B4aCustomField'
    # B4A_MENU_ITEM = 'B4aMenuItem'
    # B4A_SETTING = 'B4aSetting'
    EVALUATION_MEDICAL = 'EvaluationMedical'
    FORM_ASSET_RESULTS = 'FormAssetResults'
    FORM_RESULTS = 'FormResults'
    FORM_SPECIFICATIONS = 'FormSpecificationsV2'
    HISTORY_ENVIRONMENTAL_HEALTH = 'HistoryEnvironmentalHealth'
    HISTORY_MEDICAL = 'HistoryMedical'
    HOUSEHOLD = 'Household'
    SURVEY_DATA = 'SurveyData'
    VITALS = 'Vitals'
    OFFLINE_FORM = 'offlineForm'
    OFFLINE_FORM_REQUEST = 'offlineFormRequest'
