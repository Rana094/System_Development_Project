import json
import os


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

CHECKPOINT_DIR = os.path.join(
    BASE_DIR,
    "checkpoints"
)


os.makedirs(
    CHECKPOINT_DIR,
    exist_ok=True
)


def get_checkpoint_path(
    job_id
):

    return os.path.join(
        CHECKPOINT_DIR,
        f"{job_id}.json"
    )


def save_checkpoint(
    job_id,
    checkpoint
):

    path = get_checkpoint_path(
        job_id
    )

    temporary = (
        path + ".tmp"
    )

    with open(
        temporary,
        "w"
    ) as file:

        json.dump(
            checkpoint,
            file,
            indent=2
        )

    os.replace(
        temporary,
        path
    )


def load_checkpoint(
    job_id
):

    path = get_checkpoint_path(
        job_id
    )

    if not os.path.exists(
        path
    ):

        return None


    with open(
        path,
        "r"
    ) as file:

        return json.load(file)
