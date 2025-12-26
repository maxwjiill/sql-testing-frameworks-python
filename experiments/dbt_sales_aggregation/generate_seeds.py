import os
from pathlib import Path
import sys
import shutil

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR))

from experiments.data_generator import resolve_input_dir


def main() -> None:
    scale = os.getenv("DATA_SCALE", "small")
    input_dir = resolve_input_dir(scale)
    seeds_dir = Path(__file__).resolve().parent / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)

    for name in ("customers.csv", "products.csv", "sales.csv"):
        src = input_dir / name
        if not src.exists():
            raise FileNotFoundError(f"Missing input data: {src}")
        shutil.copyfile(src, seeds_dir / name)


if __name__ == "__main__":
    main()
