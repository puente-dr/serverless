from rest_call import rest_call

def pivot_rest_call(specifier, survey_org, custom_form_id, url="https://parseapi.back4app.com/classes/"):
    parse_df = rest_call(specifier, survey_org, custom_form_id, url)

    long_df = parse_df.melt(id_vars=)

    return long_df