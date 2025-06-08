# 智慧型 CSV 唯一鍵探索工具 (Intelligent CSV Unique Key Finder)

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-stable-brightgreen)

你是否曾面對一個陌生的 CSV 檔案，卻不知道該用哪個欄位或欄位組合來唯一識別每一筆記錄？本專案提供了一個強大且自動化的工具，能夠系統性地為您的 CSV 檔案尋找最小唯一鍵（Candidate Keys）。

它不僅僅是暴力破解，而是採用了從工業級演算法到啟發式策略的多層次回退機制，兼顧了效率與準確性。

---

## ✨ 主要特色

*   **全自動掃描**: 自動掃描當前目錄下的所有 CSV 檔案。
*   **智慧排除**: 可設定正則表達式，自動跳過不需分析的檔案 (預設排除 `*_header_*.csv`)。
*   **多重編碼支援**: 自動偵測並處理 `UTF-8` 和 `CP950` (繁體中文 Windows 預設) 編碼。
*   **多層次智慧策略**: 採用由快到慢、由簡到繁的三層回退策略，以最高效率尋找答案：
    1.  **`super_smart`**: 首先使用工業級的 `pyfd` 函式庫 (基於 TANE 演算法) 進行功能相依性探索。
    2.  **`linear`**: 若上一策略失敗，則嘗試快速的線性組合策略，處理簡單情況。
    3.  **`smart`**: 若線性策略仍失敗，則使用基於 Apriori 原理的智慧組合策略進行深度搜索。
*   **詳細日誌與報告**:
    *   生成一個主日誌檔 `uniquekey_finder.log` 記錄整體流程。
    *   為每個被分析的 CSV 檔案生成一份超詳細的獨立報告 (`<filename>_uniquekey_report.txt`)，其中**記錄了每一個被測試的欄位組合**，具備完整的可追溯性。
*   **互動式進度條**: 在進行耗時的組合搜索時，使用 `tqdm` 提供美觀且資訊豐富的進度條，讓您隨時掌握進度。

---

## ⚙️ 安裝

本專案基於 Python 3.8+，並依賴以下函式庫。

1.  **複製專案**
    ```bash
    git clone https://your-repository-url.git
    cd your-project-directory
    ```

2.  **安裝依賴**
    建議建立一個虛擬環境。本專案依賴 `pandas`, `tqdm`, 和 `pyfd`。您可以透過 `requirements.txt` 快速安裝。

    建立 `requirements.txt` 檔案，內容如下：
    ```txt
    pandas
    tqdm
    pyfd
    ```

    然後執行安裝指令：
    ```bash
    pip install -r requirements.txt
    ```
    *注意：`pyfd` 可能需要 C++ 編譯環境。如果安裝失敗，請根據您的作業系統安裝對應的 build tools。*

---

## 🚀 使用方式

使用方法非常簡單：

1.  將本專案的腳本 (`keyfinder.py`) 放在您想要分析的 CSV 檔案所在的資料夾中。
2.  打開您的終端機或命令提示字元，並切換到該資料夾。
3.  執行以下指令：
    ```bash
    python keyfinder.py
    ```

腳本將會自動開始掃描、分析，並在同一個資料夾中生成對應的日誌和報告檔案。

---

## 📊 輸出結果說明

執行完畢後，您會得到兩種類型的輸出檔案：

1.  **`uniquekey_finder.log`**
    這是主日誌檔，記錄了整個腳本的宏觀執行流程，例如：
    *   腳本何時開始與結束。
    *   掃描了哪些檔案。
    *   對每個檔案使用了哪些策略。
    *   最終的成功或失敗資訊。

2.  **`<filename>_uniquekey_report.txt`**
    這是為**每一個**被分析的 CSV 檔案生成的獨立、超詳細的分析報告。其內容包含：
    *   讀取檔案時使用的編碼。
    *   檔案的總筆數。
    *   **步驟 1**: 所有欄位根據「唯一性比例」的排序結果。
    *   **步驟 2**: 單一欄位檢查的結果。
    *   **步驟 3**: 組合探索的詳細過程。在使用 `smart` 或 `exhaustive` 策略時，這裡會**列出每一個被測試過的欄位組合**，讓分析過程完全透明。
    *   **最終結果**: 🎉 報告找到的最小唯一鍵。

    **報告範例片段：**
    ```
    --- 步驟 3 (智慧策略 - Apriori Based) ---

      >> 正在生成並測試長度為 2 的候選碼 <<
        > 測試: ['user_id', 'start_time']
        > 測試: ['user_id', 'mission_sn']
        ...

    🎉 成功！共找到 1 個最小唯一鍵: [['user_id', 'start_time', 'mission_sn']]
    ```

---

## 🧠 策略詳解

本工具的核心是其多層次策略，下表解釋了它們的區別：

| 策略 | 方法 | 優點 | 缺點 |
| :--- | :--- | :--- | :--- |
| **`super_smart`** | 功能相依性演算法 (TANE/HyFD) | **速度極快**，理論完備，專為大數據設計。 | 依賴 `pyfd` 函式庫，對資料格式要求較嚴格。 |
| **`linear`** | 貪婪啟發式搜索 | 速度快，實現簡單，能快速解決簡單問題。 | 可能會錯過由「強弱」欄位組成的複雜唯一鍵。 |
| **`smart`** | 基於 Apriori 原理的組合搜索 | **兼顧效率與完備性**，能找到線性策略錯過的解。 | 計算量大於 `linear`，但遠小於暴力破解。 |
| **`exhaustive`** | 暴力組合搜索 (備用) | **保證找到**在長度限制內的任何解。 | **極其耗時**，僅在所有其他策略失敗時作為最後手段。 |

---

## 🔧 客製化

您可以輕鬆修改腳本的行為：

*   **更改策略順序**: 在 `main()` 函數中，`DirectoryScanner` 類別的 `_get_strategy_order()` 方法定義了策略的回退順序。您可以根據需求調整它。
*   **更改排除規則**: 在 `DirectoryScanner` 的初始化方法中，可以修改 `exclude_pattern` 的正則表達式，以排除不同模式的檔案。
*   **調整最大長度**: 在 `UniqueKeyFinder` 的初始化方法中，可以修改 `max_key_length` 參數，以探索更長的組合鍵（但會增加計算時間）。

---

## 🤝 貢獻

歡迎任何形式的貢獻！如果您發現了 bug、有新的功能建議，或是想要優化演算法，請隨時提交 Pull Request 或開啟一個 Issue。

---

## 📄 授權

本專案採用 [MIT 授權](https://opensource.org/licenses/MIT)。
