import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
import pandas as pd
from data_io_manager import LocalDataHandler, PostgresDataHandler, MySQLDataHandler, S3DataHandler, MinIODataHandler
from data_cleaner import DataCleaner
import os
import tempfile

class DesignedButton(tk.Canvas):
    def __init__(self, parent, text, command=None, **kwargs):
        super().__init__(parent, width=170, height=50, bg='#4CAF50', highlightthickness=0, **kwargs)
        self.command = command
        self.text = text
        self.text_id = self.create_text(85, 25, text=self.text, fill='white', font=('Helvetica', 12, 'bold'))
        self.bind("<Button-1>", self.on_click)
        self.bind("<Enter>", self.on_enter)
        self.bind("<Leave>", self.on_leave)

    def on_click(self, event):
        if self.command:
            self.command()

    def on_enter(self, event):
        self._update_button_style('#45a049')

    def on_leave(self, event):
        self._update_button_style('#4CAF50')

    def _update_button_style(self, color):
        self.configure(bg=color)
        self.update_text()

    def update_text(self):
        self.delete(self.text_id)
        self.text_id = self.create_text(85, 25, text=self.text, fill='white', font=('Helvetica', 12, 'bold'))

class DataApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Data Management App")
        self.geometry("1000x700")

        # Initialize variables
        self.file_path = None
        self.file_extension = None
        self.df = None
        self.cleaned_df = None
        self.sort_reverse = False

        # Create UI components
        self.main_frame = tk.Frame(self, bg='#f7f7f7')
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=20)

        # Create a frame for buttons
        self.button_frame = tk.Frame(self.main_frame, bg='#f7f7f7')
        self.button_frame.pack(pady=20)

        # Add Upload Button
        self.upload_button = DesignedButton(self.button_frame, text="Upload File", command=self.upload_file)
        self.upload_button.grid(row=0, column=0, padx=10)

        # Add Download Buttons
        self.add_download_buttons()

        # Add Buttons for Data Management
        self.buttons_frame = tk.Frame(self.main_frame, bg='#f7f7f7')
        self.buttons_frame.pack(pady=20)

        self.read_button = None
        self.write_pg_button = None
        self.write_mysql_button = None
        self.write_s3_button = None
        self.write_minio_button = None
        self.clean_data_button = None

        self.table_frame = None

    def add_download_buttons(self):
        # Adding PostgreSQL button
        self.postgres_button = DesignedButton(self.button_frame, text="Download from PostgreSQL", command=self.download_postgres)
        self.postgres_button.grid(row=0, column=1, padx=10)

        # Adding MySQL button
        self.mysql_button = DesignedButton(self.button_frame, text="Download from MySQL", command=self.download_mysql)
        self.mysql_button.grid(row=0, column=2, padx=10)

        # Adding MinIO button
        self.minio_button = DesignedButton(self.button_frame, text="Download from MinIO", command=self.download_minio)
        self.minio_button.grid(row=0, column=3, padx=10)

        # Adding S3 button
        self.s3_button = DesignedButton(self.button_frame, text="Download from S3", command=self.download_s3)
        self.s3_button.grid(row=0, column=4, padx=10)

    def download_postgres(self):
        table_name = self.prompt_for_table()
        if not table_name:
            return

        format_choice = self.prompt_for_format()
        if not format_choice:
            return

        postgres_handler = PostgresDataHandler()
        data = postgres_handler.read(table_name)
        self.save_data(data, "Postgres_Data", format_choice)

    def download_mysql(self):
        table_name = self.prompt_for_table()
        if not table_name:
            return

        format_choice = self.prompt_for_format()
        if not format_choice:
            return

        mysql_handler = MySQLDataHandler()
        data = mysql_handler.read(table_name)
        self.save_data(data, "MySQL_Data", format_choice)

    def download_minio(self):
        bucket_name, object_name = self.prompt_for_bucket_and_object()
        if not bucket_name or not object_name:
            return

        minio_handler = MinIODataHandler()
        minio_handler.read(bucket_name, "/Users/ygtcan/Downloads/", object_name)

    def download_s3(self):
        bucket_name, object_name = self.prompt_for_bucket_and_object()
        if not bucket_name or not object_name:
            return

        s3_handler = S3DataHandler()
        s3_handler.read(bucket_name, "/Users/ygtcan/Downloads/", object_name)

    def save_data(self, data, base_name, file_format):
        output_dir = filedialog.askdirectory()
        if not output_dir:
            return

        local_handler = LocalDataHandler()
        local_handler.write(data, base_name, output_dir, file_format)
        messagebox.showinfo("Success", f"Data saved in {file_format.upper()} format.")

    def upload_file(self):
        self.file_path = filedialog.askopenfilename()
        if self.file_path:
            self.file_extension = self.file_path.split('.')[-1]
            self.display_buttons()

    def display_buttons(self):
        self._clear_buttons()

        if self.file_extension in ['csv', 'parquet', 'json']:
            self.read_button = DesignedButton(self.buttons_frame, text=f"Read {self.file_extension.upper()}", command=self.read_file)
            self.read_button.pack(pady=10)

        else:
            messagebox.showerror("Unsupported File", "Unsupported file type selected.")
            return

        self.clean_data_button = DesignedButton(self.buttons_frame, text="Clean Data", command=self.clean_data)
        self.clean_data_button.pack(side="left", padx=5)

        self._create_write_buttons()

    def _clear_buttons(self):
        for widget in [self.read_button, self.write_pg_button, self.write_mysql_button, self.write_s3_button, self.write_minio_button, self.clean_data_button]:
            if widget:
                widget.pack_forget()

    def _create_write_buttons(self):
        self.write_pg_button = DesignedButton(self.buttons_frame, text="Write to PostgreSQL", command=self.write_to_postgres)
        self.write_pg_button.pack(side="left", padx=5)

        self.write_mysql_button = DesignedButton(self.buttons_frame, text="Write to MySQL", command=self.write_to_mysql)
        self.write_mysql_button.pack(side="left", padx=5)

        self.write_s3_button = DesignedButton(self.buttons_frame, text="Write to S3", command=self.write_to_s3)
        self.write_s3_button.pack(side="left", padx=5)

        self.write_minio_button = DesignedButton(self.buttons_frame, text="Write to MinIO", command=self.write_to_minio)
        self.write_minio_button.pack(side="left", padx=5)

    def read_file(self):
        handler = LocalDataHandler()
        try:
            self.df = handler.read(self.file_path, self.file_extension)
            self.display_data(self.df)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read file: {e}")

    def display_data(self, df):
        if self.table_frame:
            self.table_frame.destroy()

        self.table_frame = tk.Frame(self)
        self.table_frame.pack(fill="both", expand=True)

        x_scrollbar = tk.Scrollbar(self.table_frame, orient="horizontal")
        y_scrollbar = tk.Scrollbar(self.table_frame, orient="vertical")

        self.tree = ttk.Treeview(self.table_frame, columns=list(df.columns), show="headings", yscrollcommand=y_scrollbar.set, xscrollcommand=x_scrollbar.set)
        self.columns = list(df.columns)
        self.tree['columns'] = self.columns

        for col in self.columns:
            self.tree.heading(col, text=col, command=lambda _col=col: self.sort_by(_col))
            self.tree.column(col, width=100, anchor="w")

        self.df_data = df.to_dict(orient="records")
        for row in self.df_data:
            self.tree.insert("", "end", values=list(row.values()))

        self.tree.grid(row=0, column=0, sticky="nsew")
        x_scrollbar.config(command=self.tree.xview)
        y_scrollbar.config(command=self.tree.yview)
        x_scrollbar.grid(row=1, column=0, sticky="ew")
        y_scrollbar.grid(row=0, column=1, sticky="ns")

        self.table_frame.grid_rowconfigure(0, weight=1)
        self.table_frame.grid_columnconfigure(0, weight=1)

    def sort_by(self, col):
        data = [(self.tree.item(child)["values"], child) for child in self.tree.get_children()]
        data.sort(reverse=self.sort_reverse, key=lambda t: t[0][self.columns.index(col)])
        for index, (val, child) in enumerate(data):
            self.tree.move(child, '', index)
        self.sort_reverse = not self.sort_reverse

    def clean_data(self):
        if self.df is None:
            messagebox.showerror("Error", "No data to clean.")
            return

        try:
            cleaner = DataCleaner(self.df)
            self.cleaned_df = cleaner.clean_data()
            self.display_data(self.cleaned_df)
            messagebox.showinfo("Success", "Data cleaned successfully.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to clean data: {e}")

    def write_to_postgres(self):
        if self.cleaned_df is None:
            messagebox.showerror("Error", "No cleaned data to write.")
            return

        table_name = simpledialog.askstring("Table Name", "Enter the table name:")
        if not table_name:
            return

        postgres_handler = PostgresDataHandler()
        postgres_handler.write(self.cleaned_df, table_name)
        messagebox.showinfo("Success", "Data written to PostgreSQL.")

    def write_to_mysql(self):
        if self.cleaned_df is None:
            messagebox.showerror("Error", "No cleaned data to write.")
            return
        self.cleaned_df.columns = self.cleaned_df.columns.str.strip()
        table_name = simpledialog.askstring("Table Name", "Enter the table name:")
        if not table_name:
            return

        mysql_handler = MySQLDataHandler()
        mysql_handler.write(self.cleaned_df, table_name)
        messagebox.showinfo("Success", "Data written to MySQL.")

    def write_to_s3(self):
        if self.cleaned_df is None:
            messagebox.showerror("Error", "No cleaned data to write.")
            return

        bucket_name = simpledialog.askstring("Bucket Name", "Enter the bucket name:")
        object_name = simpledialog.askstring("Object Name", "Enter the object name:")
        if not bucket_name or not object_name:
            return

        try:
            file_extension = self.file_extension.lower()  
            if file_extension not in ['csv', 'json', 'parquet']:
                messagebox.showerror("Error", "Unsupported file type.")
                return

            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_extension}") as temp_file:
                temp_path = temp_file.name

                if file_extension == 'csv':
                    self.cleaned_df.to_csv(temp_path, index=False)
                elif file_extension == 'json':
                    self.cleaned_df.to_json(temp_path, orient='records', lines=True)
                elif file_extension == 'parquet':
                    self.cleaned_df.to_parquet(temp_path)

            s3_handler = S3DataHandler()
            s3_handler.write(temp_path, bucket_name, object_name)
            messagebox.showinfo("Success", f"Data written to S3 as {file_extension.upper()}.")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to write data to S3: {e}")

        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def write_to_minio(self):
        if self.cleaned_df is None:
            messagebox.showerror("Error", "No cleaned data to write.")
            return

        bucket_name = simpledialog.askstring("Bucket Name", "Enter the bucket name:")
        object_name = simpledialog.askstring("Object Name", "Enter the object name:")
        if not bucket_name or not object_name:
            return

        try:
            file_extension = self.file_extension.lower()  
            if file_extension not in ['csv', 'json', 'parquet']:
                messagebox.showerror("Error", "Unsupported file type.")
                return
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_extension}") as temp_file:
                temp_path = temp_file.name

                if file_extension == 'csv':
                    self.cleaned_df.to_csv(temp_path, index=False)
                elif file_extension == 'json':
                    self.cleaned_df.to_json(temp_path, orient='records', lines=True)
                elif file_extension == 'parquet':
                    self.cleaned_df.to_parquet(temp_path)
                    
            minio_handler = MinIODataHandler()
            minio_handler.write(temp_path, bucket_name, object_name)
            messagebox.showinfo("Success", f"Data written to MinIO as {file_extension.upper()}.")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to write data to MinIO: {e}")

        finally:
            
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def prompt_for_table(self):
        return simpledialog.askstring("Table Name", "Enter the table name:")

    def prompt_for_format(self):
        formats = ["csv", "json", "parquet"]
        format_choice = simpledialog.askstring("Format", f"Choose format ({', '.join(formats)}):")
        if format_choice not in formats:
            messagebox.showerror("Error", "Invalid format. Please choose from csv, json, or parquet.")
            return None
        return format_choice

    def prompt_for_bucket_and_object(self):
        bucket_name = simpledialog.askstring("Bucket Name", "Enter the bucket name:")
        object_name = simpledialog.askstring("Object Name", "Enter the object name:")
        return bucket_name, object_name

if __name__ == "__main__":
    app = DataApp()
    app.mainloop()
