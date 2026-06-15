#!/bin/bash
echo "=========================="
echo "Đang khởi tạo cấu hình MinIO"
echo "=========================="

# Đọc hoặc set mặc định User/Pass
MINIO_USER=${MINIO_ROOT_USER:-"root"}
MINIO_PASS=${MINIO_ROOT_PASSWORD:-"password"}

# Kiểm tra môi trường bằng cách tìm file /.dockerenv
if [ -f /.dockerenv ]; then
    echo "-> Chạy trong Docker: Dùng URL nội bộ http://minio:9000"
    MINIO_URL="http://minio:9000"
else
    echo "-> Chạy trên Local: Dùng URL http://localhost:9000"
    MINIO_URL="http://localhost:9000"
fi

echo "Đang thiết lập alias 'minio' cho công cụ mc..."
if ! mc alias set minio $MINIO_URL $MINIO_USER $MINIO_PASS > /dev/null 2>&1; then
    echo "❌ Lỗi: Không thể chạy lệnh 'mc alias set'. Hãy chắc chắn bạn đã cài phần mềm MinIO Client (mc)!"
    exit 1
fi
echo "Thiết lập alias thành công!"

echo "=========================="
echo "Đang khởi tạo bucket"
echo "=========================="

BUCKET_LIST=("sandbox" "bronze" "silver" "gold")
for BUCKET in "${BUCKET_LIST[@]}"; do
    BUCKET_PATH="minio/$BUCKET"
    if ! mc stat "$BUCKET_PATH" > /dev/null 2>&1; then
        echo "Chưa có bucket $BUCKET"
        if [ $BUCKET == 'sandbox' ]; then
            mc mb $BUCKET_PATH
            mc ilm add --expire-days 30 $BUCKET_PATH #Cấu hình cho bucket sandbox để nó xóa file sau 30 ngày
        else
            mc mb $BUCKET_PATH
        fi
    else 
        echo "bucket $BUCKET này đã tồn tại rùi!"
    fi
done 

echo "Kiểm tra các bucket đã tồn tại chưa"
mc ls minio

echo "Kiểm tra đã cài đặt cấu hình cho sandbox chưa"
mc ilm ls minio/sandbox
