import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from data_io_manager import LocalDataHandler, PostgresDataHandler, MySQLDataHandler, S3DataHandler, MinIODataHandler
from data_cleaner import DataCleaner

class DesignedButton(tk.Canvas):
    def __init__(self, parent, text, command=None, **kwargs):
        super().__init__(parent, width=150, height=40, bg='#4CAF50', highlightthickness=0, **kwargs)
        self.command = command
        self.text = text
        self.text_id = self.create_text(75, 20, text=self.text, fill='white', font=('Helvetica', 12, 'bold'))
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
        self.text_id = self.create_text(75, 20, text=self.text, fill='white', font=('Helvetica', 12, 'bold'))

class DataApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Management App")
        self._set_window_size()

        self.file_path = None
        self.file_extension = None
        self.df = None
        self.cleaned_df = None
        self.sort_reverse = False

        self.main_frame = tk.Frame(root, bg='#f7f7f7')
        self.main_frame.pack(fill="both", expand=True, padx=20, pady=20)

        self.upload_button = DesignedButton(self.main_frame, text="Upload File", command=self.upload_file)
        self.upload_button.pack(pady=20, anchor="center")

        self.buttons_frame = tk.Frame(self.main_frame, bg='#f7f7f7')
        self.buttons_frame.pack(pady=20)

        self.read_button = None
        self.write_pg_button = None
        self.write_mysql_button = None
        self.write_s3_button = None
        self.write_minio_button = None
        self.clean_data_button = None

        self.table_frame = None

    def _set_window_size(self):
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        window_width = int(screen_width * 0.5)
        window_height = int(screen_height * 0.5)
        self.root.geometry(f"{window_width}x{window_height}")

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

        self.table_frame = tk.Frame(self.root)
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
        self._write_to_database(PostgresDataHandler, "PostgreSQL")

    def write_to_mysql(self):
        self._write_to_database(MySQLDataHandler, "MySQL")

    def write_to_s3(self):
        bucket_name, object_name = self.get_s3_minio_inputs("Enter S3 bucket name and object name:")
        if bucket_name and object_name:
            if self.file_path is None:
                messagebox.showerror("Error", "File path is missing.")
                return

            try:
                handler = S3DataHandler()
                handler.write(self.file_path, bucket_name, object_name)
                messagebox.showinfo("Success", "File uploaded to S3.")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to upload to S3: {e}")

    def write_to_minio(self):
        bucket_name, object_name = self.get_s3_minio_inputs("Enter MinIO bucket name and object name:")
        if bucket_name and object_name:
            if self.file_path is None:
                messagebox.showerror("Error", "File path is missing.")
                return

            try:
                handler = MinIODataHandler()
                handler.write(self.file_path, bucket_name, object_name)
                messagebox.showinfo("Success", "File uploaded to MinIO.")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to upload to MinIO: {e}")

    def _write_to_database(self, handler_class, db_name):
        table_name = self.get_input(f"Enter {db_name} table name:")
        if table_name:
            if self.cleaned_df is None and self.df is None:
                messagebox.showerror("Error", f"File data or table name is missing for {db_name}.")
                return

            try:
                handler = handler_class()
                handler.write(self.cleaned_df if self.cleaned_df is not None else self.df, table_name)
                messagebox.showinfo("Success", f"Data written to {db_name}.")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to write to {db_name}: {e}")

    def get_input(self, prompt):
        return simpledialog.askstring("Input", prompt)

    def get_s3_minio_inputs(self, prompt):
        dialog = tk.Toplevel(self.root)
        dialog.title("Input Required")
        dialog.geometry("350x250")  

        tk.Label(dialog, text=prompt).pack(pady=10)
        
        tk.Label(dialog, text="Bucket Name:").pack(pady=5)
        bucket_name_entry = tk.Entry(dialog, width=30)  
        bucket_name_entry.pack(pady=5)

        tk.Label(dialog, text="Object Name:").pack(pady=5)
        object_name_entry = tk.Entry(dialog, width=30)  
        object_name_entry.pack(pady=5)

        def submit():
            self.bucket_name = bucket_name_entry.get()
            self.object_name = object_name_entry.get()
            dialog.destroy()

        def cancel():
            self.bucket_name = None
            self.object_name = None
            dialog.destroy()

        button_frame = tk.Frame(dialog)
        button_frame.pack(pady=10)

        tk.Button(button_frame, text="Submit", command=submit).pack(side="left", padx=10)
        tk.Button(button_frame, text="Cancel", command=cancel).pack(side="right", padx=10)

        self.root.wait_window(dialog)
        return self.bucket_name, self.object_name

if __name__ == "__main__":
    root = tk.Tk()
    app = DataApp(root)
    root.mainloop()