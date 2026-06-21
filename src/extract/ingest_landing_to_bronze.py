import sys
import argparse
from datetime import datetime
from pathlib import Path
from src.extract.base_ingestor import BaseIngestor


def main():
    parser = argparse.ArgumentParser(
        description="Ingest dữ liệu từ Landing Zone lên Bronze Layer"
    )
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Tên nguồn dữ liệu (itviec, vietnamworks, careerviet, topcv) hoặc 'all' để chạy tất cả"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Ngày cần ingest, format YYYY-MM-DD (mặc định: hôm nay)"
    )
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, "%Y-%m-%d")

    # Xác định danh sách các nguồn cần chạy
    if args.source == "all":
        sources = BaseIngestor.VALID_SOURCES
    else:
        sources = [args.source]

    # Chạy pipeline cho từng nguồn
    for source in sources:
        print(f"\n{'='*60}")
        print(f"  Đang xử lý nguồn: {source}")
        print(f"  Ngày: {args.date}")
        print(f"{'='*60}\n")

        ingestor = BaseIngestor(source_name=source, date=date_obj)
        ingestor.run()


if __name__ == "__main__":
    main()
