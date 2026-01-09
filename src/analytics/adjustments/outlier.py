"""
Ultra-fast outlier detection for returns analysis.
4 methods: zscore, iqr, mad (Median Absolute Deviation), isolation forest
"""
import numpy as np
import pandas as pd


class OutlierDetector:
    """Fast outlier detection with multiple methods"""

    METHODS = {'zscore', 'iqr', 'mad', 'isolation'}

    def __init__(self, method: str = 'zscore', threshold: float = 3.0):
        """
        Args:
            method: 'zscore' (default, fastest), 'iqr', 'mad', 'isolation'
            threshold:
                - zscore: std multiplier (3.0 = 99.7% confidence)
                - iqr: IQR multiplier (1.5 = standard Tukey)
                - mad: MAD multiplier (2.5 = ~99% for normal dist)
                - isolation: contamination fraction (0.1 = expect 10% outliers)
        """
        if method not in self.METHODS:
            raise ValueError(f"Method must be one of {self.METHODS}")

        self.method = method
        self.threshold = threshold

    def detect(self, series: pd.Series | np.ndarray) -> np.ndarray:
        """
        Detect outliers. Returns boolean mask (True = inlier, False = outlier).

        Args:
            series: 1D array or Series

        Returns:
            Boolean mask (True = keep, False = remove)
        """
        if isinstance(series, pd.Series):
            values = series.values
        else:
            values = np.asarray(series)

        # Remove NaN/inf
        valid_mask = np.isfinite(values)

        if self.method == 'zscore':
            return self._zscore(values, valid_mask)
        elif self.method == 'iqr':
            return self._iqr(values, valid_mask)
        elif self.method == 'mad':
            return self._mad(values, valid_mask)
        elif self.method == 'isolation':
            return self._isolation(values, valid_mask)

    def _zscore(self, values: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
        """Z-score: |x - mean| / std <= threshold"""
        mask = np.ones(len(values), dtype=bool)
        if valid_mask.sum() < 2:
            return mask

        valid_vals = values[valid_mask]
        z = np.abs((valid_vals - valid_vals.mean()) / (valid_vals.std() + 1e-10))
        mask[valid_mask] = z <= self.threshold
        return mask

    def _iqr(self, values: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
        """IQR: outliers are outside [Q1 - threshold*IQR, Q3 + threshold*IQR]"""
        mask = np.ones(len(values), dtype=bool)
        if valid_mask.sum() < 4:
            return mask

        valid_vals = values[valid_mask]
        q1, q3 = np.percentile(valid_vals, [25, 75])
        iqr = q3 - q1
        lower, upper = q1 - self.threshold * iqr, q3 + self.threshold * iqr
        mask[valid_mask] = (valid_vals >= lower) & (valid_vals <= upper)
        return mask

    def _mad(self, values: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
        """MAD (Median Absolute Deviation): |x - median| / MAD <= threshold"""
        mask = np.ones(len(values), dtype=bool)
        if valid_mask.sum() < 2:
            return mask

        valid_vals = values[valid_mask]
        median = np.median(valid_vals)
        mad = np.median(np.abs(valid_vals - median))

        if mad < 1e-10:  # All values identical
            return mask

        mask[valid_mask] = np.abs(valid_vals - median) / mad <= self.threshold
        return mask

    def _isolation(self, values: np.ndarray, valid_mask: np.ndarray) -> np.ndarray:
        """Isolation Forest (requires scikit-learn)"""
        try:
            from sklearn.ensemble import IsolationForest
        except ImportError:
            # Fallback to zscore if sklearn not available
            return self._zscore(values, valid_mask)

        mask = np.ones(len(values), dtype=bool)
        if valid_mask.sum() < 5:
            return mask

        valid_vals = values[valid_mask].reshape(-1, 1)
        iso = IsolationForest(contamination=self.threshold, random_state=42)
        preds = iso.fit_predict(valid_vals)  # -1 = outlier, 1 = inlier
        mask[valid_mask] = preds == 1
        return mask

    def filter_series(self, series: pd.Series) -> pd.Series:
        """Remove outliers from series (set to NaN)"""
        mask = self.detect(series)
        filtered = series.copy()
        filtered[~mask] = np.nan
        return filtered

    def __repr__(self) -> str:
        return f"OutlierDetector(method={self.method}, threshold={self.threshold})"