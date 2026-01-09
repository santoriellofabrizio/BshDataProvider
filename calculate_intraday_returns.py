"""
Intraday Returns Analysis Tool
Visualizza ritorni cumulativi e componenti di adjustment (TER, Dividend, FX, FX Forward)
Supporta modalità live (Bloomberg) e storica (dati passati)
"""
import matplotlib

from analytics.adjustments.outlier import OutlierDetector

matplotlib.use('TkAgg')
import asyncio
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from dateutil.utils import today

from xbbg import blp
from analytics.adjustments import Adjuster
from analytics.adjustments.dividend import DividendComponent
from analytics.adjustments.fx_spot import FxSpotComponent
from analytics.adjustments.fx_forward_carry import FxForwardCarryComponent
from analytics.adjustments.ter import TerComponent
from core.holidays.holiday_manager import HolidayManager
from interface.bshdata import BshData

warnings.filterwarnings("ignore")

# Color palette for multiple tickers
COLORS = ['steelblue', 'darkorange', 'seagreen', 'crimson', 'purple', 'gold', 'teal', 'pink']


class DebugReturnsAnalyzer:
    """Analyzer per ritorni intraday con adjustment components"""

    def __init__(self, tickers: list, frequency: str, number_of_days: int = 3,
                 cumulative: bool = True, live_mode: bool = False,
                 outlier_detection: str | bool = False):
        """
        Args:
            tickers: Lista di ticker ETF
            frequency: Frequenza dati (es. "15m", "1h", "1d")
            number_of_days: Giorni storici da caricare
            cumulative: Se True mostra ritorni cumulativi, else ritorni giornalieri
            live_mode: Se True attende aggiornamenti Bloomberg live
            outlier_detection: False (disabilitato), True (zscore), o string method
                             Methods: 'zscore' (fastest), 'iqr', 'mad', 'isolation'
        """
        self.tickers = tickers
        self.frequency = frequency
        self.number_of_days = number_of_days
        self.cumulative = cumulative
        self.live_mode = live_mode
        self.intraday = "d" not in frequency.lower()

        # Setup outlier detection
        if outlier_detection is False:
            self.outlier_detector = None
        elif outlier_detection is True:
            self.outlier_detector = OutlierDetector(method='zscore', threshold=3.0)
        elif isinstance(outlier_detection, str):
            self.outlier_detector = OutlierDetector(method=outlier_detection, threshold=3.0)
        else:
            raise ValueError("outlier_detection must be False, True, or method string")

        self.api = None
        self.etf = None
        self.fx = None
        self.adj = None
        self.columns = None
        self.bbg_map = None

        self.fig = None
        self.ax_returns = None
        self.ax_adj = None
        self.title_returns = None
        self.title_adj = None

        self.bars_dict = {}
        self.adj_bars_dict = {}
        self.ref_prices = None
        self.n_periods = None
        self.n_tickers = None

    async def load_data(self):
        """Carica dati storici e setup adjuster"""
        print("[*] Loading data...")

        self.api = BshData()
        start = HolidayManager().add_business_days(today(), -self.number_of_days)
        yesterday = HolidayManager().add_business_days(today(), -1)
        currencies = ['USD', 'GBP', 'CHF', 'AUD', 'DKK', 'HKD', 'NOK', 'PLN', 'SEK', 'CNY', 'JPY', 'CNH', 'CAD', 'INR', 'BRL']

        # Load ETF data
        self.etf = self.api.market.get(
            type="ETP",
            start=start,
            end=yesterday,
            ticker=self.tickers,
            market="ETFP",
            frequency=self.frequency,
            source="timescale",
        )

        # Load FX data
        self.fx = self.api.market.get(
            type="CURRENCYPAIR",
            start=start,
            end=yesterday,
            id=[f"EUR{c}" for c in currencies],
            frequency=self.frequency,
            source="timescale",
            fallbacks=[{"source": "bloomberg"}]
        )

        # Filter intraday hours if needed
        if self.intraday:
            self.etf = self.etf.between_time('10:00', '17:00')
            self.fx = self.fx.between_time('10:00', '17:00')

        # Setup adjuster
        self.adj = (
            Adjuster(prices=self.etf, intraday=self.intraday)
            .add(TerComponent(self.api.info.get_ter(ticker=self.tickers)))
            .add(DividendComponent(
                self.api.info.get_dividends(start=start, ticker=self.tickers),
                fx_prices=self.fx
            ))
            .add(FxSpotComponent(
                self.api.info.get_fx_composition(ticker=self.tickers, fx_fxfwrd="fx"),
                fx_prices=self.fx
            ))
            .add(FxForwardCarryComponent(
                self.api.info.get_fx_composition(ticker=self.tickers, fx_fxfwrd="fxfwrd"),
                self.api.market.get_daily_fx_forward(quoted_currency=currencies, start=start),
                "1M",
                fx_spot_prices=self.fx
            ))
        )

        # Map columns
        self.columns = list(self.etf.columns)
        self.bbg_map = {f"{t} IM Equity": c for t, c in zip(self.tickers, self.columns)}
        self.bbg_map.update({f"EUR{c} Curncy": f"EUR{c}" for c in currencies})

        self.ref_prices = self.etf.copy()
        self.n_periods = len(self.ref_prices)
        self.n_tickers = len(self.columns)

        outlier_msg = f" | Outlier detection: {self.outlier_detector}" if self.outlier_detector else ""
        print(f"[✓] Loaded {self.etf.shape} | Columns: {', '.join(self.columns)}{outlier_msg}")

    def setup_plot(self):
        """Inizializza figure e subplot"""
        print("[*] Setting up plot...")

        plt.ion()
        self.fig, (self.ax_returns, self.ax_adj) = plt.subplots(
            2, 1,
            figsize=(18, 12),
            gridspec_kw={'height_ratios': [1, 1]}
        )

        x = np.arange(self.n_periods)
        width = 0.8 / self.n_tickers

        # ===== TOP SUBPLOT: Cumulative Returns =====
        for i, col in enumerate(self.columns):
            offset = (i - (self.n_tickers - 1) / 2) * width
            color = COLORS[i % len(COLORS)]
            bars = self.ax_returns.bar(x + offset, np.zeros(self.n_periods), width, label=col, color=color)
            self.bars_dict[col] = {'bars': bars, 'color': color}

        self.ax_returns.set_ylabel("Return (BP)")
        self.ax_returns.set_xticks(x)
        self.ax_returns.set_xticklabels([t.strftime("%H:%M") for t in self.ref_prices.index], rotation=45, ha='right')
        self.ax_returns.axhline(y=0, color="black", linewidth=0.5)
        self.ax_returns.legend(loc='upper left', fontsize=9)
        self.title_returns = self.ax_returns.set_title("Returns - Waiting for data...")
        self.ax_returns.bar_labels = []

        # ===== BOTTOM SUBPLOT: Raw Returns & Adjustments =====
        width_adj = 0.2
        for i, col in enumerate(self.columns):
            offset = (i - (self.n_tickers - 1) / 2) * width_adj
            color = COLORS[i % len(COLORS)]

            bars_raw = self.ax_adj.bar(
                x + offset - width_adj*1.0, np.zeros(self.n_periods), width_adj*0.8,
                label=f"{col} (Raw)", color=color, alpha=0.7
            )
            bars_ter = self.ax_adj.bar(
                x + offset - width_adj*0.4, np.zeros(self.n_periods), width_adj*0.8,
                label="TER" if i == 0 else "", color='red', alpha=0.5
            )
            bars_div = self.ax_adj.bar(
                x + offset + width_adj*0.2, np.zeros(self.n_periods), width_adj*0.8,
                label="Dividend" if i == 0 else "", color='green', alpha=0.5
            )
            bars_fx = self.ax_adj.bar(
                x + offset + width_adj*0.8, np.zeros(self.n_periods), width_adj*0.8,
                label="FX Spot" if i == 0 else "", color='blue', alpha=0.5
            )
            bars_fxfwd = self.ax_adj.bar(
                x + offset + width_adj*1.4, np.zeros(self.n_periods), width_adj*0.8,
                label="FX Fwd" if i == 0 else "", color='purple', alpha=0.5
            )

            self.adj_bars_dict[col] = {
                'raw': bars_raw,
                'ter': bars_ter,
                'dividend': bars_div,
                'fx': bars_fx,
                'fxfwd': bars_fxfwd,
                'color': color
            }

        self.ax_adj.set_xlabel("From Time")
        self.ax_adj.set_ylabel("Return Components (BP)")
        self.ax_adj.set_xticks(x)
        self.ax_adj.set_xticklabels([t.strftime("%H:%M") for t in self.ref_prices.index], rotation=45, ha='right')
        self.ax_adj.axhline(y=0, color="black", linewidth=0.5)

        # Custom legend
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor=COLORS[i % len(COLORS)], alpha=0.7, label=col) for i, col in enumerate(self.columns)]
        legend_elements += [Patch(facecolor='red', alpha=0.5, label='TER'),
                            Patch(facecolor='green', alpha=0.5, label='Dividend'),
                            Patch(facecolor='blue', alpha=0.5, label='FX Spot'),
                            Patch(facecolor='purple', alpha=0.5, label='FX Fwd')]
        self.ax_adj.legend(handles=legend_elements, loc='upper left', fontsize=9, ncol=2)

        self.title_adj = self.ax_adj.set_title("Raw Returns & Adjustment Components - Waiting for data...")

        plt.tight_layout()
        plt.show(block=False)
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

        print("[✓] Plot ready")

    def update_plots(self, live_etf: pd.Series, live_fx: pd.Series, update_num: int):
        """Aggiorna entrambi i subplot con nuovi dati"""
        # Calculate returns
        rets_clean = self.adj.clean_returns(
            live_prices=live_etf,
            fx_prices=live_fx,
            cumulative=self.cumulative
        ) * 1e4

        rets_raw = self.adj.get_raw_returns(
            live_prices=live_etf,
            cumulative=self.cumulative
        ) * 1e4

        breakdown = self.adj.get_breakdown()

        # ===== UPDATE TOP SUBPLOT =====
        self._update_returns_subplot(rets_clean, update_num)

        # ===== UPDATE BOTTOM SUBPLOT =====
        self._update_adjustment_subplot(rets_raw, breakdown)

        # Draw
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()
        plt.pause(0.001)

    def _update_returns_subplot(self, rets_clean: pd.DataFrame, update_num: int):
        """Aggiorna il subplot dei ritorni cumulativi"""
        all_vals = []

        for col in self.columns:
            ret_series = rets_clean[col]

            # Apply outlier detection if enabled
            if self.outlier_detector:
                ret_filtered = self.outlier_detector.filter_series(ret_series)
            else:
                ret_filtered = ret_series

            ret_reindexed = ret_filtered.reindex(self.ref_prices.index).fillna(0).values
            all_vals.extend(ret_reindexed)

            # Update bar heights
            for bar, h in zip(self.bars_dict[col]['bars'], ret_reindexed):
                bar.set_height(h)

        # Remove old labels
        if hasattr(self.ax_returns, 'bar_labels'):
            for label in self.ax_returns.bar_labels:
                label.remove()

        # Create new labels
        self.ax_returns.bar_labels = []
        for col in self.columns:
            ret_series = rets_clean[col]

            if self.outlier_detector:
                ret_filtered = self.outlier_detector.filter_series(ret_series)
            else:
                ret_filtered = ret_series

            ret_reindexed = ret_filtered.reindex(self.ref_prices.index).fillna(0).values
            color = self.bars_dict[col]['color']

            for bar, h in zip(self.bars_dict[col]['bars'], ret_reindexed):
                if abs(h) > 1:  # Show only significant values
                    lbl = self.ax_returns.text(
                        bar.get_x() + bar.get_width() / 2, h,
                        f'{h:.1f}',
                        ha='center', va='bottom' if h > 0 else 'top',
                        fontsize=6, color=color, weight='bold'
                    )
                    self.ax_returns.bar_labels.append(lbl)

        # Adjust y limits
        if all_vals:
            all_vals = np.array(all_vals)
            all_vals = all_vals[np.isfinite(all_vals)]
            if len(all_vals) > 0:
                margin = max(abs(all_vals.max()), abs(all_vals.min())) * 0.1 + 1
                self.ax_returns.set_ylim(all_vals.min() - margin, all_vals.max() + margin)

        # Update title
        mode = "Cumulative" if self.cumulative else "Period"
        self.title_returns.set_text(f"{mode} Returns | {datetime.now():%H:%M:%S} | #{update_num}")

    def _update_adjustment_subplot(self, rets_raw: pd.DataFrame, breakdown: dict):
        """Aggiorna il subplot dei componenti di adjustment"""
        all_vals_adj = []

        for col in self.columns:
            # Raw returns
            raw_series = rets_raw[col] if col in rets_raw.columns else pd.Series(0, index=self.ref_prices.index)

            if self.outlier_detector:
                raw_filtered = self.outlier_detector.filter_series(raw_series)
            else:
                raw_filtered = raw_series

            raw_reindexed = raw_filtered.reindex(self.ref_prices.index).fillna(0).values
            all_vals_adj.extend(raw_reindexed)

            for bar, h in zip(self.adj_bars_dict[col]['raw'], raw_reindexed):
                bar.set_height(h)

            # Get adjustment components from breakdown dict
            ter_df = breakdown.get("TerComponent", pd.DataFrame())
            div_df = breakdown.get("DividendComponent", pd.DataFrame())
            fx_df = breakdown.get("FxSpotComponent", pd.DataFrame())
            fxfwd_df = breakdown.get("FxForwardCarryComponent", pd.DataFrame())

            # Extract series for this ticker and convert to BP
            ter_series = ter_df[col]*1e4 if col in ter_df.columns else pd.Series(0, index=self.ref_prices.index)
            div_series = div_df[col]*1e4 if col in div_df.columns else pd.Series(0, index=self.ref_prices.index)
            fx_series = fx_df[col]*1e4 if col in fx_df.columns else pd.Series(0, index=self.ref_prices.index)
            fxfwd_series = fxfwd_df[col]*1e4 if col in fxfwd_df.columns else pd.Series(0, index=self.ref_prices.index)

            ter_reindexed = ter_series.reindex(self.ref_prices.index).fillna(0).values
            div_reindexed = div_series.reindex(self.ref_prices.index).fillna(0).values
            fx_reindexed = fx_series.reindex(self.ref_prices.index).fillna(0).values
            fxfwd_reindexed = fxfwd_series.reindex(self.ref_prices.index).fillna(0).values

            all_vals_adj.extend(ter_reindexed)
            all_vals_adj.extend(div_reindexed)
            all_vals_adj.extend(fx_reindexed)
            all_vals_adj.extend(fxfwd_reindexed)

            # Update bars
            for bar, h in zip(self.adj_bars_dict[col]['ter'], ter_reindexed):
                bar.set_height(h)
            for bar, h in zip(self.adj_bars_dict[col]['dividend'], div_reindexed):
                bar.set_height(h)
            for bar, h in zip(self.adj_bars_dict[col]['fx'], fx_reindexed):
                bar.set_height(h)
            for bar, h in zip(self.adj_bars_dict[col]['fxfwd'], fxfwd_reindexed):
                bar.set_height(h)

        # Adjust y limits
        if all_vals_adj:
            all_vals_adj = np.array(all_vals_adj)
            all_vals_adj = all_vals_adj[np.isfinite(all_vals_adj)]
            if len(all_vals_adj) > 0:
                margin = max(abs(all_vals_adj.max()), abs(all_vals_adj.min())) * 0.1 + 1
                self.ax_adj.set_ylim(all_vals_adj.min() - margin, all_vals_adj.max() + margin)

        # Update title
        self.title_adj.set_text(f"Raw Returns & Adjustment Components | {datetime.now():%H:%M:%S}")

    async def run_live(self):
        """Modalità live: aspetta aggiornamenti Bloomberg"""
        if not self.live_mode:
            raise ValueError("Live mode non abilitato")

        print(f"[*] Starting live mode | Tickers: {', '.join(self.columns)}\n")

        live_etf, live_fx, n = pd.Series(dtype=float), pd.Series(dtype=float), 0

        try:
            async for u in blp.live(list(self.bbg_map.keys()), flds=["MID"]):
                if (p := u.get("MID")) is None:
                    continue
                if (col := self.bbg_map.get(u.get("TICKER", ""))) is None:
                    continue

                (live_etf if "Equity" in u.get("TICKER", "") else live_fx).__setitem__(col, p)
                n += 1

                # Check if all tickers have live prices
                if not all(c in live_etf for c in self.columns):
                    continue

                # Update plots
                self.update_plots(live_etf, live_fx, n)

                # Console output
                price_str = " | ".join([f"{col}: {live_etf[col]:.4f}" for col in self.columns])
                sys.stdout.write(f"\r{datetime.now():%H:%M:%S} | {price_str} | #{n}")
                sys.stdout.flush()

        except KeyboardInterrupt:
            print(f"\n[✓] Stopped after {n} updates")
            self._save_plot()
            plt.ioff()
            plt.show()

    async def run_historical(self):
        """Modalità storica: usa prezzi finali della serie"""
        print(f"[*] Starting historical mode | Data shape: {self.etf.shape}\n")

        live_etf = self.etf.iloc[-1]
        live_fx = self.fx.iloc[-1]

        self.update_plots(live_etf, live_fx, 1)

        price_str = " | ".join([f"{col}: {live_etf[col]:.4f}" for col in self.columns])
        print(f"{datetime.now():%H:%M:%S} | {price_str} | #1")

        try:
            while True:
                # Processa eventi GUI di matplotlib
                self.fig.canvas.flush_events()
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            print(f"\n[✓] Closed")
            self._save_plot()
            plt.ioff()
            plt.show()

    def _save_plot(self):
        """Salva la figura"""
        filename = f"returns_analysis_{datetime.now():%Y%m%d_%H%M%S}.png"
        self.fig.savefig(filename, dpi=100)
        print(f"[✓] Saved to {filename}")

    async def run(self):
        """Main entry point"""
        await self.load_data()
        self.setup_plot()

        if self.live_mode:
            await self.run_live()
        else:
            await self.run_historical()


async def main():
    """Entry point"""
    # Configuration
    tickers = ['AWSRIA', 'AWSRIE']
    frequency = "1d"
    number_of_days = 10
    cumulative = True
    live_mode = True

    # Outlier detection options:
    # - False: disabilitato (default)
    # - True: z-score, threshold=3.0
    # - 'zscore', 'iqr', 'mad', 'isolation': metodo specifico
    outlier_detection = 'zscore'

    # Run analyzer
    analyzer = DebugReturnsAnalyzer(
        tickers=tickers,
        frequency=frequency,
        number_of_days=number_of_days,
        cumulative=cumulative,
        live_mode=live_mode,
        outlier_detection=outlier_detection
    )
    await analyzer.run()


if __name__ == "__main__":
    asyncio.run(main())