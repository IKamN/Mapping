import tkinter as tk
from tkinter import ttk
from tkinter import filedialog
from dataclasses import dataclass
import yaml
import os


class Config:
    def __init__(self, dict_config: dict):
        for key, value in dict_config.items():
            setattr(self, key, value)


# config_path = os.path.join(os.path.dirname(os.path.realpath(__package__)), 'config.yml')

def get_interface(config_path):

    def finish_work():
        root.destroy()

    def submit():
        values = {}
        for i in range(10):
            entry = entries[i]
            field_name = labels[i]
            value = entry.get()
            values[field_name] = value

        with open(config_path, "w", encoding="utf-8") as file:
            yaml.dump(values, file, allow_unicode=True, default_style="'")

        root.protocol("WM_DELETE_WINDOW", finish_work())

    root = tk.Tk()

    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    window_width = 800
    window_height = 300
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2


    root.geometry(f"{window_width}x{window_height}+{x}+{y}")

    default_values = {}
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            default_values = yaml.safe_load(file)
    except FileNotFoundError:
        print("File config.yml not found")

    config = Config(default_values)
    len_config = len(default_values)
    labels = list(default_values.keys())

    entries = []
    for i in range(0, len_config):
        label = tk.Label(root, text=labels[i], font=("Arial", 10))
        label.grid(row=i, column=0, sticky=tk.E)
        entry = tk.Entry(root, width=60)
        entry.grid(row=i, column=1, ipadx = 8 )
        field_name = labels[i]
        entry.insert(0, default_values.get(field_name, ""))
        entries.append(entry)


    def browse_directory():
        return filedialog.askdirectory()
        # entry_directory.delete(0, tk.END)
        # entry_directory.insert(0, directory)

    def entered(event):
        button_browse["text"] = "..."

    def left(event):
        button_browse["text"] = "..."
    #
    # label_directory = tk.Label(root, text="file_dir")
    # label_directory.grid(row=0, column=0, sticky=tk.E)
    # # entry_directory = tk.Entry(root, width=50)
    # button_browse = tk.Button(root, text="Browse", command=browse_directory)
    # button_browse.grid(row=0, column=3)
    #
    #
    # label_load_type = tk.Label(root, text="loadType")
    # label_load_type.grid(row=8, column=0, sticky=tk.E)
    # load_type_var = tk.StringVar(root)

    # Button "Browse"
    button_browse = ttk.Button(root, text="...", command=browse_directory)
    button_browse.grid(row=0, column=3, ipadx=8, sticky="E")
    button_browse.bind("<Enter>", "...")
    button_browse.bind("<Leave>", "...")

    # Button "Create"
    create_button = ttk.Button(root, text="Create", command=submit)
    create_button.grid(row=14, columnspan=3, ipady=5)
    create_button.bind("<Enter>", "Create")
    create_button.bind("<Leave>", "Create")


    root.mainloop()

# get_interface("D:\Projects\VTB\mappings\interface\config.yml")

