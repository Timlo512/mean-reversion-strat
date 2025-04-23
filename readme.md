# Mean Reversion Strategy

This repository is designed to execute, backtest, forward test, and analyze a mean reversion strategy. It also aims to implement common mean reversion techniques.

## Strategies

### 1. Nasdaq Mean Reversion Strategy
The first strategy focuses on implementing a mean reversion strategy for Nasdaq stocks. This involves identifying overbought or oversold conditions and executing trades based on statistical mean reversion principles.

#### Data Collection for Backtest

To perform backtesting, I will collect minute-level data for the Nasdaq index, TQQQ, and SQQQ. This data will serve as the foundation for analyzing market conditions, identifying mean reversion opportunities, and testing the strategy's performance under historical scenarios.

## Setup

### Prerequisites
To run this project, ensure you have the following installed and configured:

1. **PostgreSQL (Version > 15)**  
   - Install PostgreSQL version 15 or higher. 
   - Ensure the database is running and accessible.  
   - Create a database for storing market data and strategy results.
   - Install via [homebrew](https://formulae.brew.sh/formula/postgresql@15)

2. **IB API Gateway**  
   - Install and configure the Interactive Brokers (IB) API Gateway.  
   - Ensure the gateway is running and connected to your IB account.  
   - Refer to the [IB API documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/cpapi-v1/#gw-step-one) for setup instructions.

3. **Python Virtual Environment**  
   - Set up a virtual environment to manage Python dependencies.  
   - Use the following commands to create and activate a virtual environment:
     ```bash
     python3 -m venv .venv # or python -m venv .venv
     source venv/bin/activate  # On Mac/Linux
     # For Windows:
     # venv\Scripts\activate
     ```
   - Install the required dependencies using `pip`:
     ```bash
     pip install -r requirements.txt
     ```

Once the prerequisites are installed, you can proceed with configuring and running the project.



## Reference
(https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/#market-data-31)