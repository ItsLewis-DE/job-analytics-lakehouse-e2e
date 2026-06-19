import argparse
from datetime import datetime
from src.transform.job_standardizer import (
    JobStandardizer,
    TOPCV_COLUMN_MAPPING,
    ITVIEC_COLUMN_MAPPING,
    VIETNAMWORKS_COLUMN_MAPPING
)

VALID_SOURCES = ['itviec', 'topcv', 'vietnamworks']

MAPPING_DICT = {
    "topcv": TOPCV_COLUMN_MAPPING,
    "itviec": ITVIEC_COLUMN_MAPPING,
    "vietnamworks": VIETNAMWORKS_COLUMN_MAPPING
}

def main():
    parser = argparse.ArgumentParser(
        description="Transform dữ liệu từ Bronze Layer lên Silver Layer"
    )
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Tên nguồn dữ liệu (itviec, topcv, vietnamworks) hoặc 'all' để chạy tất cả"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Ngày cần xử lý, format YYYY-MM-DD (mặc định: hôm nay)"
    )
    args = parser.parse_args()

    date_obj = datetime.strptime(args.date, "%Y-%m-%d")

    if args.source == "all":
        sources = VALID_SOURCES
    else:
        if args.source not in VALID_SOURCES:
            raise ValueError(f"Source '{args.source}' không hợp lệ. Các nguồn hỗ trợ: {VALID_SOURCES}")
        sources = [args.source]

    # Chạy pipeline chuẩn hóa cho từng nguồn
    for source in sources:
        print(f"\n{'='*60}")
        print(f"  ĐANG CHUẨN HÓA DỮ LIỆU TỪ BRONZE LÊN SILVER")
        print(f"  Nguồn: {source}")
        print(f"  Ngày: {args.date}")
        print(f"{'='*60}\n")

        column_mapping = MAPPING_DICT[source]

        standardizer = JobStandardizer(
            source_name=source, 
            column_mapping=column_mapping, 
            date=date_obj
        )
        standardizer.run()

if __name__ == "__main__":
    main()
