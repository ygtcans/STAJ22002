import pandas as pd
from src.data_io_manager import LocalDataHandler, S3DataHandler
from src.db_connections import AWSClient

def main():
    data_handler = LocalDataHandler()
    #CSV dosyasından veri okuma
    csv_df = data_handler.read("data/tiktok_dataset.csv","csv")
    print(csv_df.head(1))
    #CSV dosyasından okuduğumuz veriyi json ve parquete yazalım.
    data_handler.write(csv_df,"parquet_data","s","parquet")
    data_handler.write(csv_df,"json_data","s","json")
    #Şimdi bu verilerin yazılıp yazılmadığını kontrol edeceğiz.
    parquet_df = data_handler.read("data/parquet_data_16-08-2024_16-07-48.parquet", "parquet")
    print(parquet_df.head(1))
    json_df = data_handler.read("data/json_data_16-08-2024_16-07-48.json", "json")
    print(json_df.head(1))
    
    #Elimizde ki bu dosyaları şimdi amazon s3 ye yazacağız.
    aws_client = AWSClient()
    #Şimdi ilk olarak aws e bağlanalım.
    aws_client.connect()
    #Bağlantıdan sonra bir bucket oluşturalım.
    aws_client.create_bucket("ygtcans-test-bucket")
    #Oluşturduğumuz bucket içerisne elimizde dosyaları yazalım/yükleyelim.
    s3 = S3DataHandler()
    s3.write("data","ygtcans-test-bucket","data")
    #Yüklediğimiz dosyaları okuyarak/indirerek task i tamamlayalım.
    s3.read("ygtcans-test-bucket","data_s3","data","data/",multiple=True)
    

if __name__ == '__main__':
    main()