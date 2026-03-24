from src.extract.api_client import StackExchangeClient
from src.utils.file_utils import save_json

def main():
    client = StackExchangeClient()

    response = client.call_api(
        endpoint="questions",
        params={
            "pagesize": 100,
            "order": "desc",
            "sort": "creation"
        }
    )

    questions = response.get("items", [])

    output_path = save_json(
        data=questions,
        output_dir="data/bronze/questions",
        prefix="questions"
    )

    print(f"Saved {len(questions)} questions to {output_path}")


if __name__ == "__main__":
    main()   