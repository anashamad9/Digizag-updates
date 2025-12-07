import pandas as pd
import numpy as np

class OnlineEWMAClassifier:
    def __init__(self, alpha=0.1, z_threshold=0.3):
        """
        alpha: smoothing factor (0<alpha<=1), higher = more responsive
        z_threshold: number of std deviations away for positive/negative
        """
        self.alpha = alpha
        self.z_threshold = z_threshold
        
        self.mean = None
        self.var = None  # EWMA variance

    def update(self, x):
        """
        Process a new data point.
        Returns: 'positive', 'negative', or 'neutral'
        """

        # Initialization for the first data point (WARNING: MAY LEAD TO GIGANTIC Z-SCORE)
        if self.mean is None:
            self.mean = x
            self.var = 0.0
            return "neutral"

        # Compute z-score BEFORE updating distribution
        std = (self.var ** 0.5) if self.var > 0 else 1e-9
        z = (x - self.mean) / std

        # print(f"{x}: {z}")

        # Classification
        if z > self.z_threshold:
            label = "positive"
        elif z < -self.z_threshold:
            label = "negative"
        else:
            label = "neutral"

        
        if abs(z) <= 3:
            # Update EWMA mean

            prev_mean = self.mean
            self.mean = self.alpha * x + (1 - self.alpha) * self.mean

            # Update EWMA variance (exponential smoothing)
            self.var = self.alpha * (x - prev_mean)**2 + (1 - self.alpha) * self.var

        print(f"mean: {self.mean}\nstd: {self.var ** 0.5}")

        return label, z

    def set_mean(self, mean):
        self.mean = mean
    def set_std(self, var):
        self.var = var**2

    def get_mean(self):
        return self.mean
    def get_std(self):
        return self.var ** 0.5

df = pd.DataFrame({})

vals = """578.77
581.31
673.86
440.52
175.37
210.04
237.19
252.3
225.67
224.07
266.08
146.11
281.15
226.97
147.58
451.57
233.07
226.66
266.08
301.45
302.18
303.45
350.15
291.75
360.15
398
412.6
311.67
503.17
455.68"""

vals = vals.split("\n")
print(vals)
vals = list(map(lambda x: float(x), vals))

df.index = np.arange(1,31,1)

classifier = OnlineEWMAClassifier()
classifier.set_mean(327.8206667)
classifier.set_std(131.1704296)

values = {"Values": [],
          "Class": [],
          "CumMean": [],
          "CumStd": [],
          "ZScore": []}

for val in vals:
    values["Values"].append(val)
    values["CumMean"].append(classifier.get_mean())
    values["CumStd"].append(classifier.get_std())
    label, z_score = classifier.update(val)
    values["Class"].append(label)
    values["ZScore"].append(z_score)

df = pd.DataFrame(values, index=df.index)
print(df)