# -*- coding: utf-8 -*-
"""
高效唯一鍵探索演算法腳本 (V7.2 - 真正完整版)

本腳本將整個流程重構成物件導向結構，並根據 linter (如 pylint) 的
建議進行了優化，以提升程式碼品質和可讀性。
此版本確保所有方法均已完整實現，無任何省略。
"""
import pandas as pd
import os
import re
import itertools
import time
import logging
import math
from enum import Enum, auto
from typing import List, Optional, Set, FrozenSet, TextIO

# --- 檢查並匯入可選的函式庫 ---
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from pyfd import hyfd
    PYFD_AVAILABLE = True
except ImportError:
    PYFD_AVAILABLE = False

# --- 全局日誌設定 ---
LOG_FILENAME = 'uniquekey_finder.log'
if os.path.exists(LOG_FILENAME):
    os.remove(LOG_FILENAME)
logger = logging.getLogger('UniqueKeyFinder')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_FILENAME, encoding='utf-8')
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class Strategy(Enum):
    """定義可用的唯一鍵探索策略。"""
    LINEAR = auto()
    EXHAUSTIVE = auto()
    SMART = auto()
    SUPER_SMART = auto()

# pylint: disable=too-many-instance-attributes
class UniqueKeyFinder:
    """
    代表對單一 CSV 檔案的完整分析過程。
    這個類別封裝了載入資料、執行不同策略以及生成報告的所有邏輯。
    """
    def __init__(self, file_path: str, max_key_length: int = 5):
        """初始化 UniqueKeyFinder。"""
        self.file_path: str = file_path
        self.filename: str = os.path.basename(file_path)
        self.report_filename: str = f"{os.path.splitext(self.filename)[0]}_uniquekey_report.txt"
        self.max_key_length: int = max_key_length
        self.df: Optional[pd.DataFrame] = None
        self.total_count: int = 0
        self.sorted_columns: List[str] = []
        self.non_unique_L1: List[str] = []
        self.solutions: List[List[str]] = []
        self.report_file: Optional[TextIO] = None

    def run(self, strategy_order: List[Strategy]) -> None:
        """執行完整的分析流程，按照指定的策略順序。"""
        with open(self.report_filename, 'w', encoding='utf-8') as self.report_file:
            logger.info(f"--- 開始處理檔案: {self.filename} (報告將寫入: {self.report_filename}) ---")
            
            if not self._load_data() or self.df is None:
                return

            if self._prepare_and_check_single_keys():
                logger.info(f"單一欄位即為唯一鍵，無需進一步組合。")
            else:
                for strategy in strategy_order:
                    if self._run_strategy(strategy):
                        break
                else:
                    logger.info(f"所有策略均未能為 '{self.filename}' 找到唯一鍵。")
        
        logger.info(f"--- 檔案處理完成: {self.filename} ---")

    def _load_data(self) -> bool:
        """載入 CSV 檔案，自動處理 utf-8 和 cp950 編碼。"""
        logger.info(f"正在讀取檔案 '{self.filename}' 到記憶體中...")
        self.report_file.write("  (正在讀取檔案...)\n")
        try:
            self.df = pd.read_csv(self.file_path, dtype=str, on_bad_lines='warn', encoding='utf-8')
            read_msg = "檔案讀取完成 (使用 utf-8)。"
        except UnicodeDecodeError:
            logger.info("utf-8 解碼失敗，轉而嘗試 cp950 編碼...")
            try:
                self.df = pd.read_csv(self.file_path, dtype=str, on_bad_lines='warn', encoding='cp950')
                read_msg = "檔案讀取完成 (使用 cp950)。"
            except (IOError, pd.errors.ParserError) as e:
                msg = f"❌ 讀取檔案失敗 (utf-8 和 cp950 均失敗): {e}"
                logger.error(msg)
                self.report_file.write(msg + "\n")
                return False
        except (IOError, pd.errors.ParserError) as e:
            msg = f"❌ 讀取檔案時發生錯誤: {e}"
            logger.error(msg)
            self.report_file.write(msg + "\n")
            return False
        
        logger.info(read_msg)
        self.report_file.write(f"  ({read_msg})\n")
        self.total_count = len(self.df)
        if self.total_count == 0:
            logger.warning("檔案為空，無法分析。")
            self.report_file.write("檔案為空。\n")
            return False
        self.report_file.write(f"總筆數: {self.total_count:,}\n\n")
        return True

    def _prepare_and_check_single_keys(self) -> bool:
        """準備分析，計算唯一性比例並檢查單一欄位唯一鍵。"""
        uniqueness_report = []
        for col in self.df.columns:
            unique_count = self.df[col].nunique()
            uniqueness_report.append({'column': col, 'uniqueness_ratio': unique_count / self.total_count})
        self.sorted_columns = pd.DataFrame(uniqueness_report).sort_values(by='uniqueness_ratio', ascending=False)['column'].tolist()
        
        self.report_file.write("--- 步驟 1: 欄位按唯一性比例排序 ---\n" + str(self.sorted_columns) + "\n\n")
        self.report_file.write("--- 步驟 2: 檢查單一欄位 ---\n")
        
        for col in self.sorted_columns:
            if self.df[col].nunique() == self.total_count:
                self.solutions = [[col]]
                self._log_and_report_success()
                return True
            self.non_unique_L1.append(col)
            
        self.report_file.write("沒有單一欄位是唯一鍵，開始組合探索...\n\n")
        return False

    def _run_strategy(self, strategy: Strategy) -> bool:
        """根據傳入的策略類型，分派到對應的執行方法。"""
        self.report_file.write("="*60 + "\n")
        self.report_file.write(f"🚀 探索唯一鍵 (策略: {strategy.name})\n")
        self.report_file.write("="*60 + "\n\n")

        strategy_map = {
            Strategy.SUPER_SMART: self._run_strategy_super_smart,
            Strategy.LINEAR: self._run_strategy_linear,
            Strategy.SMART: self._run_strategy_smart,
            Strategy.EXHAUSTIVE: self._run_strategy_exhaustive
        }
        
        solutions = strategy_map.get(strategy, lambda: None)()
        
        if solutions:
            self.solutions = solutions
            self._log_and_report_success()
            return True
        
        logger.info(f"'{strategy.name}' 策略未能找到唯一鍵。")
        self.report_file.write(f"❌ '{strategy.name}' 策略未能在最大長度 {self.max_key_length} 內找到唯一鍵。\n")
        return False

    def _run_strategy_super_smart(self) -> Optional[List[List[str]]]:
        """使用 pyfd 函式庫執行功能相依性探索。"""
        if not PYFD_AVAILABLE:
            logger.error("'pyfd' 函式庫未安裝，無法執行 super_smart 策略。")
            return None
        try:
            logger.info("呼叫 pyfd 引擎進行功能相依性探索...")
            df_no_na = self.df.fillna('[PYFD_NULL]')
            fds = hyfd(df_no_na, max_k=self.max_key_length)
            
            all_columns = set(self.df.columns)
            candidate_keys = [list(ant) for ant, dep in fds.items() if set(dep) == all_columns - set(ant)]
            
            minimal_keys = []
            candidate_keys.sort(key=len)
            for key in candidate_keys:
                if not any(set(sol).issubset(set(key)) for sol in minimal_keys):
                    minimal_keys.append(key)
            return minimal_keys if minimal_keys else None
        except Exception as e: # pylint: disable=broad-except-clause
            logger.error(f"pyfd 執行時發生錯誤: {e}")
            return None

    def _run_strategy_linear(self) -> Optional[List[List[str]]]:
        """執行快速的線性組合策略。"""
        base_combination = []
        for col in self.sorted_columns:
            base_combination.append(col)
            if len(base_combination) < 2: continue
            if len(base_combination) > self.max_key_length: break
            
            logger.info(f"  測試組合: {base_combination}...")
            if len(self.df[base_combination].drop_duplicates()) == self.total_count:
                return [base_combination]
        return None

    def _run_strategy_smart(self) -> Optional[List[List[str]]]:
        """執行基於 Apriori 原理的智慧組合策略。"""
        Lk_minus_1 = {frozenset([col]) for col in self.non_unique_L1}
        solutions = []
        for k in range(2, self.max_key_length + 1):
            self.report_file.write(f"\n  >> 正在生成並測試長度為 {k} 的候選碼 <<\n")
            
            Ck = self._generate_candidates(Lk_minus_1, k)
            if not Ck:
                self.report_file.write("    無法生成更多候選碼，搜索結束。\n")
                break
            
            Lk = self._test_candidates(Ck, solutions, k)
            if not Lk:
                self.report_file.write("    本輪未發現非唯一鍵，無需繼續迭代。\n")
                break
            Lk_minus_1 = Lk
            
        return solutions if solutions else None
    
    def _generate_candidates(self, Lk_minus_1: Set[FrozenSet[str]], k: int) -> Set[FrozenSet[str]]:
        """Apriori-gen 函式的實現，用於生成候選碼。"""
        return {s1.union(s2) for i, s1 in enumerate(sorted(list(Lk_minus_1))) for s2 in sorted(list(Lk_minus_1))[i+1:] if len(s1.union(s2)) == k}

    def _test_candidates(self, Ck: Set[FrozenSet[str]], solutions: List[List[str]], k: int) -> Set[FrozenSet[str]]:
        """測試一組候選碼，返回其中的非唯一鍵。"""
        Lk = set()
        iterable = tqdm(sorted(list(Ck)), desc=f"測試長度 {k} ", unit=" 組") if TQDM_AVAILABLE else sorted(list(Ck))
        for candidate_set in iterable:
            candidate = list(candidate_set)
            if any(set(sol).issubset(set(candidate)) for sol in solutions):
                self.report_file.write(f"    > 跳過 (是已知解的超集): {candidate}\n")
                continue
            
            if TQDM_AVAILABLE: iterable.set_description(f"測試長度 {k}: {str(candidate)}")
            self.report_file.write(f"    > 測試: {candidate}\n")
            
            if len(self.df[candidate].drop_duplicates()) == self.total_count:
                solutions.append(candidate)
                logger.info(f"  > 找到一個最小唯一鍵: {candidate}")
                self.report_file.write(f"    >> 找到解: {candidate}\n")
            else:
                Lk.add(candidate_set)
        return Lk

    def _run_strategy_exhaustive(self) -> Optional[List[List[str]]]:
        """執行全面的暴力組合策略（備用）。"""
        logger.warning("正在執行 exhaustive 策略，這可能會非常耗時。")
        for k in range(2, self.max_key_length + 1):
            num_combinations = math.comb(len(self.sorted_columns), k)
            self.report_file.write(f"\n  >> 正在測試所有長度為 {k} 的組合 (總計: {num_combinations:,} 種) <<\n")
            
            combinations_iterator = itertools.combinations(self.sorted_columns, k)
            iterable = tqdm(combinations_iterator, total=num_combinations, desc=f"測試長度 {k} ", unit=" 組") if TQDM_AVAILABLE else combinations_iterator
            
            for columns_to_test in iterable:
                current_combination = list(columns_to_test)
                if TQDM_AVAILABLE: iterable.set_description(f"測試長度 {k}: {str(current_combination)}")
                self.report_file.write(f"    > 測試: {current_combination}\n")
                if len(self.df[current_combination].drop_duplicates()) == self.total_count:
                    if TQDM_AVAILABLE: iterable.close()
                    return [current_combination]
        return None

    def _log_and_report_success(self) -> None:
        """統一處理找到解後的日誌和報告寫入。"""
        result_msg = f"🎉 成功！共找到 {len(self.solutions)} 個最小唯一鍵: {self.solutions}"
        logger.info(result_msg)
        self.report_file.write("\n" + result_msg + "\n")

class DirectoryScanner:
    """掃描目錄，為每個符合條件的檔案建立並執行 UniqueKeyFinder。"""
    def __init__(self, directory: str = '.', exclude_pattern: str = r'_header_\d+\.csv$'):
        self.directory = directory
        self.exclude_pattern = re.compile(exclude_pattern, re.IGNORECASE)
        self.strategy_order = self._get_strategy_order()

    def _get_strategy_order(self) -> List[Strategy]:
        """根據可用函式庫決定策略執行順序。"""
        order = []
        if PYFD_AVAILABLE:
            order.append(Strategy.SUPER_SMART)
        order.append(Strategy.LINEAR)
        order.append(Strategy.SMART)
        return order

    def scan_and_process(self) -> None:
        """掃描並處理目錄中的所有合格檔案。"""
        logger.info("===== 腳本開始執行 (V7.2 - 完整物件導向版) =====")
        if not TQDM_AVAILABLE:
            logger.warning("'tqdm' 模組未安裝。進度條將不會顯示。")
        if not PYFD_AVAILABLE:
            logger.warning("'pyfd' 函式庫未安裝，'super_smart' 策略將被跳過。")

        logger.info(f"開始掃描目錄 '{os.path.abspath(self.directory)}'...")
        
        try:
            files_to_process = [f for f in os.listdir(self.directory) if f.lower().endswith('.csv') and not self.exclude_pattern.search(f)]
        except FileNotFoundError:
            logger.error(f"錯誤：找不到目錄 '{os.path.abspath(self.directory)}'。")
            return
        
        if not files_to_process:
            logger.warning("在當前目錄下未找到任何符合條件的 CSV 檔案。")
            return

        logger.info(f"找到 {len(files_to_process)} 個檔案待處理: {files_to_process}")

        for filename in files_to_process:
            file_path = os.path.join(self.directory, filename)
            finder = UniqueKeyFinder(file_path)
            finder.run(strategy_order=self.strategy_order)
        
        logger.info("===== 所有檔案均已分析完畢！ =====")

if __name__ == "__main__":
    scanner = DirectoryScanner()
    scanner.scan_and_process()
