"""
Radial Basis Function Network implementation
Ruivaldo Neto - rneto at rneto.net
"""

import numpy as np

from detector import SuperDetector


class RBF(SuperDetector):
    """Radial Basis Function with Markov Chain detector."""

    def __init__(self, min_window_size=1, sigma=0.5, threshold=0.75):
        super().__init__()

        self.min_window_size = min_window_size
        self.sigma = sigma
        self.threshold = threshold

        self.centers = []
        self.actual_center = None
        self.window = []

    def run(self, prediction):
        warning_status = False
        drift_status = False

        self.window.append(prediction)

        # Window has been filled
        if len(self.window) >= self.min_window_size:
            activation_threshold = self.threshold
            activated_center = None
            activation = 0.0

            window_mean = np.mean(self.window)

            for center in self.centers:
                distance = np.sqrt(np.float_power(window_mean - center, 2))
                activation = np.exp(
                    # ϕ(r) = exp(- r²/2σ²)
                    -(np.float_power(distance, 2.0))
                    / (2.0 * np.float_power(self.sigma, 2.0))
                )

                if activation >= activation_threshold:
                    activated_center = center
                    activation_threshold = activation

            # no center activates, so we have a new center
            if activated_center is None:
                self.centers.append(window_mean)
                activated_center = window_mean

            # if no center had been activated before
            if self.actual_center is None:
                self.actual_center = window_mean

            # if actual_center is not the activated_center, drift, drift!
            if self.actual_center != activated_center:
                self.actual_center = activated_center
                drift_status = True

            # A new window emerges
            self.window = []

        return warning_status, drift_status

    def reset(self):
        super().reset()

        self.centers = []
        self.actual_center = None
        self.window = []

    def get_settings(self):
        return [
            str(self.min_window_size)
            + "."
            + str(self.sigma)
            + "."
            + str(self.threshold),
            "min_window_size:"
            + str(self.min_window_size)
            + ", "
            + "sigma:"
            + str(self.sigma).upper()
            + ", "
            + "threshold:"
            + str(self.threshold).upper(),
        ]
