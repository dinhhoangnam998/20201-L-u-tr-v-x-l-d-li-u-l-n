# BTL môn học Lưu trữ và xử lí dữ liệu lớn
## Đặt vấn đề
Phân loại review (bình luận), đánh giá trên trang website thương mại điện tử (ở đây là amazon), xem review có hữu ích hay không.  
Một review được xem là hữu ích nếu như nó phản ánh được thực tế sản phẩm.
Ta cần phân loại những review này với những review mang tính spam, giả mạo,...

## Ý tưởng giải quyết
Lấy thông tin về các review đã có sẵn trên trang web, các review này đã được những khách hàng khác đánh giá (vote) là hữu ích hay không. Sau đó đưa dữ liệu vào để huấn luyện một AI model.  
Model này sẽ được dùng để dự đoán, phân loại các review sau này.

Tuy nhiên, tập dữ liệu về các review là rất lớn, nên cần phải xử lý song song phân tán trên một cụm các máy. Để làm được điều này, ta sử dụng các công nghệ lưu trữ và xử lý dữ liệu lớn, được đề cập trong môn học.

## Công nghệ sử dụng
- HDFS: Hadoop Distributed File System
- Apache Spark (ở đây sử dụng pyspark)
  + Spark SQL
  + Spark ML
  + Spark Streaming 
- Scrapy
- Kafka


## Cài đặt môi trường
- cài đặt các thư viện theo lệnh sau:
`pip install -r requirements.txt`

## Quá trình thực hiện
- Setup HDFS, Spark, ...
- Download tập dữ liệu, đẩy vào HDFS
  - Tập dữ liệu: [Amazon Review Data (2018)](https://nijianmo.github.io/amazon/index.html)
- Chạy Spark thống kê dữ liệu để lấy các tiêu chí phù hợp cho việc huấn luyện mô hình AI
- Chạy Spark để huấn luyện mô hình AI
- Xây dựng hệ thống pipeline:
  + Crawl dữ liệu về, đẩy vào kafka
  + Spark Streaming đọc dữ liệu từ kafka, dự đoán liên tục
  + Show kết quả
(Xem các file how_to_run.txt và how_to_run.md trong các module để chạy các tác vụ)

