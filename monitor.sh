#!/bin/bash
# Monitor agents in real-time

echo "🤖 Agent Monitor - Refresh every 5s (Ctrl+C to stop)"
echo "=================================================="
echo ""

while true; do
    clear
    echo "🤖 MULTI-AGENT MONITOR - $(date '+%H:%M:%S')"
    echo "=================================================="
    echo ""
    
    # Show agent_status.md
    if [ -f "agent_status.md" ]; then
        cat agent_status.md
    else
        echo "Waiting for agents to start..."
    fi
    
    echo ""
    echo "=================================================="
    echo "📁 Files created by agents:"
    echo ""
    
    # List new files
    [ -f "src/backtest/soccer_backtest.py" ] && echo "  ✅ src/backtest/soccer_backtest.py"
    [ -f "src/optimization/param_optimizer.py" ] && echo "  ✅ src/optimization/param_optimizer.py"
    [ -f "config/optimal_params.json" ] && echo "  ✅ config/optimal_params.json"
    [ -f "tests/test_soccer_trader.py" ] && echo "  ✅ tests/test_soccer_trader.py"
    
    echo ""
    echo "[Refreshing in 5s... Press Ctrl+C to stop]"
    
    sleep 5
done
