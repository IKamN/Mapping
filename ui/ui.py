import sys
import tkinter as tk
from tkinter import filedialog, messagebox
import yaml
import typing

if typing.TYPE_CHECKING:
    from app.app import App


class Config:
    pass

config = Config()

labels = [
    "file_dir",
    "subo_name",
    "id_ris",
    "mapping_version",
    "system_target",
    "loadType",
    "docs",
    "developer",
    "database",
    "topic"
]

def setup_ui(app: "App", config_path):

    def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            root.destroy()
            sys.exit(0)

    def submit():
        values: dict = {}
        for i in range(10):
            entry = entries[i]
            field_name: str = labels[i]
            value = entry.get()
            values[field_name]: str = value

        for key, value in values.items():
            setattr(config, key, value)

        app.config = config
        with open(config_path, "w", encoding="utf-8") as file:
            yaml.dump(values, file, allow_unicode=True, default_style="'")
        root.destroy()

    def browse():
        import os
        directory = os.path.dirname(filedialog.askopenfilename())
        entries[0].delete(0, tk.END)
        entries[0].insert(0, directory)

    root = tk.Tk()
    root.title("Autogen")
    window_width = 650
    window_height = 280
    root.geometry(f"{window_width}x{window_height}+{210}+{210}")
    root.resizable(False, False)
    root.config(bg="#C8FDF1")

    default_values: dict = {}
    try:
        with open(config_path, "r", encoding="utf-8") as file:
            default_values = yaml.safe_load(file)
    except FileNotFoundError:
        print("File config.yml not found")

    len_config = len(default_values)
    entries: list = []
    for i in range(0, len_config):
        label = tk.Label(root, text=labels[i], font=("Arial", 12), bg="#C8FDF1")
        label.grid(row=i, column=0, stick="E", padx=10, sticky="wens")
        entry = tk.Entry(root, width=50)
        entry.grid(row=i, column=1, ipadx = 8, sticky="wens")
        field_name = labels[i]
        entry.insert(0, default_values.get(field_name, ""))
        entries.append(entry)

    # Button "Browse"
    button_browse = tk.Button(root, text="...", command=browse)
    button_browse.grid(row=0, column=4, sticky="w")

    # Button "Create"
    create_button = tk.Button(root, text="Create", command=submit)
    create_button.grid(row=19, columnspan=3)

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()