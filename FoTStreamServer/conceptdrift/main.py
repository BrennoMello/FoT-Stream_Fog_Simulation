import os

import pandas as pd
from matplotlib import pyplot as plt
from os.path import dirname

# Baseados em estatística - média
from algorithms.cusum import CUSUM
from algorithms.page_hinkley import PH
from algorithms.ewma import EWMA

# Baseados em janela
from algorithms.adwin import ADWINChangeDetector
from algorithms.seq_drift2 import SeqDrift2ChangeDetector
from algorithms.hddm_a import HDDM_A_test


DETECTORS = (
    CUSUM,
    PH,
    EWMA,
    ADWINChangeDetector,
    SeqDrift2ChangeDetector,
    HDDM_A_test,
)

DATASET_PATH = os.path.join(dirname(dirname(__file__)), "intel-lab-dataset/dataSet_temp.txt")

raw_dataset = pd.read_csv(DATASET_PATH, delim_whitespace=True, header=0)
temperatures = raw_dataset.iloc[:, -1].dropna().head(5000).values

# No Wavelet
for detector in DETECTORS:
    detector_instance = detector()

    drift_indexes = []
    for index, value in enumerate(temperatures):
        _, is_drift = detector_instance.run(value)

        if is_drift:
            drift_indexes.append(index)
            detector_instance.reset()


    plt.ylim((0, 50))
    plt.suptitle('Without wavelet')
    plt.title(str(detector))
    plt.plot(temperatures)

    for drift_index in drift_indexes:
        plt.axvline(drift_index, linestyle='--', linewidth=1, color='r')

    plt.show()

# With wavelets
import pywt
temperatures, _ = pywt.dwt(temperatures, 'db1')

for detector in DETECTORS:
    detector_instance = detector()

    drift_indexes = []
    for index, value in enumerate(temperatures):
        _, is_drift = detector_instance.run(value)

        if is_drift:
            drift_indexes.append(index)
            detector_instance.reset()


    plt.ylim((0, 50))
    plt.suptitle('With wavelet')
    plt.title(str(detector))
    plt.plot(temperatures)

    for drift_index in drift_indexes:
        plt.axvline(drift_index, linestyle='--', linewidth=1, color='r')

    plt.show()