import asyncio
import glob
import json
import os


# NOTE(Kontrolix): We must do that here before mergify settings are loaded from env var
if not os.environ.get("MERGIFYENGINE_OPENAI_API_TOKEN"):
    if not os.environ.get("OPENAI_API_KEY"):
        raise AttributeError(
            "Either MERGIFYENGINE_OPENAI_API_TOKEN or OPENAI_API_KEY env var must be set with a valid OpenAI api key",
        )
    os.environ["MERGIFYENGINE_OPENAI_API_TOKEN"] = os.environ["OPENAI_API_KEY"]

from mergify_engine.log_embedder import github_action
from mergify_engine.log_embedder import log as logm
from mergify_engine.log_embedder import openai_api


MODEL_TRAINING_DIR = "tools/model_training"

SAMPLE_LOGS_DIR = os.path.join(MODEL_TRAINING_DIR, "sample_logs/*.txt")
TRAINING_DATA_DIR = os.path.join(MODEL_TRAINING_DIR, "training_data")


async def extract_data_from_log(
    log: logm.Log,
) -> list[github_action.ExtractedDataObject]:
    async with openai_api.OpenAIClient() as openai_client:
        return await github_action.extract_data_from_log(
            openai_client,
            log,
        )


def get_cleaned_log_path(sample_log_path: str) -> str:
    return os.path.join(
        TRAINING_DATA_DIR,
        os.path.basename(sample_log_path),
    )


def get_cleaned_log(sample_log_path: str) -> tuple[str, bool]:
    cleaned_log_path = get_cleaned_log_path(sample_log_path)

    with open(sample_log_path) as f:
        log = logm.Log.from_content(f.read())
        cleaned_log = github_action.get_cleaned_log(log)

    try:
        with open(cleaned_log_path) as f:
            is_cleaned_log_new = f.read() != cleaned_log
    except FileNotFoundError:
        with open(cleaned_log_path, "w") as f:
            f.write(cleaned_log)
        is_cleaned_log_new = True

    return cleaned_log, is_cleaned_log_new


async def get_extracted_data(
    sample_log_path: str,
    is_cleaned_log_new: bool,
) -> list[github_action.ExtractedDataObject]:
    json_path = os.path.splitext(get_cleaned_log_path(sample_log_path))[0] + ".json"

    # NOTE(Kontrolix): We remove the file here in case of a new cleaned log
    # even if we overwrite the json file later, because in case of failure of the api
    # call to openAI to get the json, next time we come here cleaned log will no longer be
    # new and will therefore not force the recall of openAi
    if is_cleaned_log_new:
        try:
            os.remove(json_path)
        except FileNotFoundError:
            pass

    try:
        with open(json_path) as f:
            return json.load(f)
    except FileNotFoundError:
        with open(sample_log_path) as f:
            log = logm.Log.from_content(f.read())

        extracted_data = await extract_data_from_log(log)
        with open(json_path, "w") as f:
            json.dump(extracted_data, f, indent=4)

        return extracted_data


async def main() -> None:
    fine_tuning_data = []
    for sample_log_path in glob.glob(SAMPLE_LOGS_DIR):
        cleaned_log, is_cleaned_log_new = get_cleaned_log(sample_log_path)

        extracted_data = await get_extracted_data(sample_log_path, is_cleaned_log_new)

        query = github_action.get_completion_query(cleaned_log)

        query["messages"].append(
            openai_api.ChatCompletionMessage(
                role="assistant",
                content=json.dumps(extracted_data),
            ),
        )

        fine_tuning_data.append(query["messages"])

    with open(os.path.join(MODEL_TRAINING_DIR, "fine_tuning.jsonl"), "w") as f:
        f.writelines(
            [json.dumps({"messages": data}) + "\n" for data in fine_tuning_data],
        )
        f.write("\n")


if __name__ == "__main__":
    asyncio.run(main())
