import random
import math
from abc import ABC, abstractmethod


class LoadGenerator(ABC):
    def __init__(self, time: int, max_threshold: int | None = None):
        self.time = time
        self.max_threshold = max_threshold
    
    @abstractmethod
    def generate(self) -> list[int]:
        pass


class RandomLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, magnitude: int, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
        self.magnitude = magnitude

    def generate(self) -> list[int]:
        values = []
        val = self.target_tps
        for i in range(0, self.time):
            val += random.randrange(-self.magnitude, self.magnitude)
            if self.max_threshold is not None and val > self.max_threshold:
                val = self.max_threshold
            values.append(val)
        return [abs(int(val)) for val in values]


class IncreaseLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, magnitude: int, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
        self.magnitude = magnitude

    def generate(self) -> list[int]:
        values = []
        val = self.target_tps
        for i in range(0, self.time):
            val += random.randrange(int(-self.magnitude * (1 / 30)), int(self.magnitude * (1 / 22)))
            if self.max_threshold is not None and val > self.max_threshold:
                val = self.max_threshold
            values.append(val)
        return [abs(int(val)) for val in values]


class DecreaseLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, magnitude: int, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
        self.magnitude = magnitude
    
    def generate(self) -> list[int]:
        values = []
        val = self.target_tps
        for i in range(0, self.time):
            val += random.randrange(int(-self.magnitude * (1 / 21)), int(self.magnitude * (1 / 28)))
            if self.max_threshold is not None and val > self.max_threshold:
                val = self.max_threshold
            values.append(val)
        return [abs(int(val)) for val in values]


class CosineLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, cosine_period: int, mean_input_rate: int,
                 max_divergence: int, max_noise: int, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
        self.cosine_period = cosine_period
        self.mean_input_rate = mean_input_rate
        self.max_divergence = max_divergence
        self.max_noise = max_noise
    
    def generate(self) -> list[int]:
        values = []
        for i in range(0, self.time):
            period = (2 * math.pi / self.cosine_period)
            value = self.mean_input_rate + self.max_divergence * math.cos(period * i + math.pi)
            value += random.random() * (2 * self.max_noise) - self.max_noise
            if self.max_threshold is not None and value > self.max_threshold:
                value = self.max_threshold
            values.append(value)
        return [abs(int(val)) for val in values]


class StepPatternLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, initial_round_length: int, regular_round_length: int,
                 round_rates: list[int], max_noise: int = 0, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
        self.initial_round_length = initial_round_length
        self.regular_round_length = regular_round_length
        self.round_rates = round_rates
        self.max_noise = max_noise
    
    def generate(self) -> list[int]:
        values = []
        for i in range(0, self.time):
            round_index = 0
            if i > self.initial_round_length:
                round_index = 1 + (i - self.initial_round_length) // self.regular_round_length
            
            value = self.round_rates[round_index] if len(self.round_rates) > round_index else 0
            value += random.random() * (2 * self.max_noise) - self.max_noise
            if self.max_threshold is not None and value > self.max_threshold:
                value = self.max_threshold
            values.append(max(0, value))
        return [abs(int(val)) for val in values]


class ConstantLoadGenerator(LoadGenerator):
    def __init__(self, target_tps: int, time: int, max_threshold: int | None = None):
        super().__init__(time, max_threshold)
        self.target_tps = target_tps
    
    def generate(self) -> list[int]:
        return [self.target_tps] * self.time


class LoadSchedule:

    GENERATORS: dict[str, type[LoadGenerator]] = {
        "constant": ConstantLoadGenerator,
        "increasing": IncreaseLoadGenerator,
        "decreasing": DecreaseLoadGenerator,
        "cosine": CosineLoadGenerator,
        "step": StepPatternLoadGenerator,
        "random": RandomLoadGenerator,
    }
    
    def __init__(self, values: list[int], step_size: int):
        """
        Args:
            values: List of TPS values from a LoadGenerator
            step_size: Duration in seconds for each TPS value
        """
        self.values = values
        self.step_size = step_size
    
    @classmethod
    def from_generator(cls, generator_name: str, step_size: int, **kwargs) -> "LoadSchedule":
        """
        Create a LoadSchedule from a generator name and parameters.
        
        Example:
            LoadSchedule.from_generator("cosine", step_size=15,
                target_tps=1000, time=20, cosine_period=10,
                mean_input_rate=1000, max_divergence=500, max_noise=50)
        """
        if generator_name not in cls.GENERATORS:
            available = ", ".join(cls.GENERATORS.keys())
            raise ValueError(f"Unknown generator '{generator_name}'. Available: {available}")
        generator = cls.GENERATORS[generator_name](**kwargs)
        values = generator.generate()
        return cls(values, step_size)
    
    @classmethod
    def from_config_file(cls, config_path: str, target_tps: int, time: int) -> "LoadSchedule":
        """Load schedule from a YAML/JSON config file."""
        import yaml
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        profile = config.pop("profile")
        step_size = config.pop("step_size", 5)
        
        # Inject runtime values
        config["target_tps"] = target_tps
        config["time"] = time
        
        return cls.from_generator(profile, step_size, **config)
    
    def get_tps(self, elapsed_seconds: float) -> int:
        index = int(elapsed_seconds // self.step_size)
        if index < 0:
            return self.values[0]
        if index >= len(self.values):
            return self.values[-1]
        return self.values[index]
    
    def total_duration(self) -> int:
        return len(self.values) * self.step_size
    
    def __iter__(self):
        for i, tps in enumerate(self.values):
            yield i * self.step_size, tps
    
    def __len__(self):
        return len(self.values)
