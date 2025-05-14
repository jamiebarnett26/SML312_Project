import pandas as pd
import json

def safe_parse_json(json_str):
    try:
        json_str = json_str.replace("'", '"')
        return json.loads(json_str)
    except Exception as e:
        return []

def clean_text_messages(input_file="textMessages.csv", output_file="textMessages.csv"):
    df = pd.read_csv(input_file, engine='python', on_bad_lines='skip')

    if 'participants' not in df.columns:
        raise KeyError("The 'participants' column is missing from the CSV.")

    df['participants'] = df['participants'].fillna('[]').apply(safe_parse_json)

    exploded_df = df.explode('participants').reset_index(drop=True)

    exploded_df['participants'] = exploded_df['participants'].apply(
        lambda x: x if isinstance(x, dict) else {}
    )
    participant_data = pd.json_normalize(exploded_df['participants'])

    exploded_df.drop(columns=['participants'], inplace=True)
    cleaned_df = pd.concat([exploded_df, participant_data], axis=1)

    cleaned_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    clean_text_messages()
