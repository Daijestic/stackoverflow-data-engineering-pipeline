from src.extract.api_client import StackExchangeClient
from src.utils.file_utils import save_json

def main():
    client = StackExchangeClient()

    response = client.call_api(
        endpoint="answers",
        params={
            "pagesize": 100,
            "order": "desc",
            "sort": "creation"
        }
    )

    answers = response.get("items", [])

    output_path = save_json(
        data=answers,
        output_dir="data/bronze/answers",
        prefix="answers"
    )

    print(f"Saved {len(answers)} answers to {output_path}")

if __name__ == "__main__":
    main()