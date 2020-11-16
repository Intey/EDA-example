from dataclasses import dataclass


@dataclass
class Store:
    processed: int = 0
    distributed: int = 0
    generated: int = 0
    in_progress: int = 0

    def finish_process_one(self):
        self.processed += 1
        self.in_progress -= 1

    def start_process_one(self):
        self.in_progress += 1

    def add_generated(self):
        self.generated += 1

    def add_distributed(self):
        self.distributed += 1

    def __str__(self):
        str_ = "=" * 80 + "\n<br>"
        str_ += f"{self.processed=}\n<br>"
        str_ += f"{self.distributed=}\n<br>"
        str_ += f"{self.generated=}\n<br>"
        str_ += f"{self.in_progress=}\n<br>"
        str_ += "=" * 80
        return str_
