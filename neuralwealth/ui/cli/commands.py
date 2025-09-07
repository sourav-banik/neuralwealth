import click
from typing import Dict, List
import requests
import json
from datetime import datetime

@click.group()
def cli():
    """NeuralWealth CLI - Command line interface for portfolio management"""
    pass

@cli.command()
@click.option('--session-id', '-s', help='Session ID', default=None)
def status(session_id):
    """Get current portfolio status"""
    try:
        if not session_id:
            # Create new session
            response = requests.get("http://localhost:8000/session/create")
            session_id = response.json()['session_id']
            click.echo(f"Created new session: {session_id}")
        
        # Get portfolio status
        response = requests.get(
            "http://localhost:8000/portfolio/status",
            params={'session_id': session_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            click.echo("Portfolio Status:")
            click.echo(f"Total Value: ${data['total_value']:,.2f}")
            click.echo(f"Cash: ${data['cash']:,.2f}")
            
            click.echo("\nPositions:")
            for asset, position in data['positions'].items():
                click.echo(f"  {asset}: {position['quantity']} shares (${position['value']:,.2f})")
            
            click.echo("\nWeights:")
            for asset, weight in data['weights'].items():
                click.echo(f"  {asset}: {weight:.1%}")
            
            click.echo("\nPerformance:")
            for period, perf in data['performance'].items():
                click.echo(f"  {period}: {perf:.2%}")
                
        else:
            click.echo(f"Error: {response.text}", err=True)
            
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server. Is it running?", err=True)

@cli.command()
@click.option('--session-id', '-s', help='Session ID', required=True)
@click.option('--strategy', '-st', help='Strategy preferences (JSON)', default='{}')
def rebalance(session_id, strategy):
    """Trigger portfolio rebalancing"""
    try:
        strategy_prefs = json.loads(strategy)
        response = requests.post(
            "http://localhost:8000/portfolio/rebalance",
            json=strategy_prefs,
            params={'session_id': session_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            click.echo("Rebalancing Results:")
            click.echo(f"Status: {data['status']}")
            click.echo(f"Message: {data['message']}")
            
            click.echo("\nNew Weights:")
            for asset, weight in data['new_weights'].items():
                click.echo(f"  {asset}: {weight:.1%}")
            
            click.echo("\nOrders Executed:")
            for order in data['orders_executed']:
                click.echo(f"  {order['action']} {order['quantity']} {order['asset']}")
                
        else:
            click.echo(f"Error: {response.text}", err=True)
            
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON format for strategy", err=True)
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server", err=True)

@cli.command()
@click.option('--session-id', '-s', help='Session ID', default=None)
def strategies(session_id):
    """List available strategies"""
    try:
        response = requests.get(
            "http://localhost:8000/strategies",
            params={'session_id': session_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            click.echo("Available Strategies:")
            
            for strategy in data['strategies']:
                click.echo(f"\n{strategy['name']} (ID: {strategy['id']})")
                click.echo(f"  Description: {strategy['description']}")
                click.echo(f"  Confidence: {strategy['confidence']:.0%}")
                click.echo(f"  Sharpe Ratio: {strategy['performance']['sharpe']:.2f}")
                click.echo(f"  Max Drawdown: {strategy['performance']['max_drawdown']:.1%}")
                click.echo(f"  Assets: {', '.join(strategy['assets'])}")
                
        else:
            click.echo(f"Error: {response.text}", err=True)
            
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server", err=True)

@cli.command()
@click.argument('message')
@click.option('--session-id', '-s', help='Session ID', default=None)
def chat(message, session_id):
    """Send chat query to KG-RAG"""
    try:
        response = requests.post(
            "http://localhost:8000/chat/query",
            json={"message": message},
            params={'session_id': session_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            click.echo(f"Response: {data['response']}")
            if 'session_id' in data:
                click.echo(f"Session ID: {data['session_id']}")
        else:
            click.echo(f"Error: {response.text}", err=True)
            
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server", err=True)

@cli.command()
@click.option('--session-id', '-s', help='Session ID', default=None)
def scenarios(session_id):
    """List available crash scenarios"""
    try:
        response = requests.get(
            "http://localhost:8000/simulation/scenarios",
            params={'session_id': session_id}
        )
        
        if response.status_code == 200:
            data = response.json()
            click.echo("Available Crash Scenarios:")
            
            for scenario in data['scenarios']:
                click.echo(f"\n{scenario['name']} (ID: {scenario['id']})")
                click.echo(f"  Description: {scenario['description']}")
                click.echo(f"  Max Drawdown: {scenario['max_drawdown']:.1%}")
                click.echo(f"  Duration: {scenario['duration_days']} days")
                click.echo(f"  Recovery: {scenario['recovery_days']} days")
                
        else:
            click.echo(f"Error: {response.text}", err=True)
            
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server", err=True)

@cli.command()
@click.option('--session-id', '-s', help='Session ID', required=True)
@click.option('--output', '-o', help='Output file', default='portfolio_report.html')
def report(session_id, output):
    """Generate portfolio report"""
    try:
        # Get portfolio data
        portfolio_response = requests.get(
            "http://localhost:8000/portfolio/status",
            params={'session_id': session_id}
        )
        
        strategies_response = requests.get(
            "http://localhost:8000/strategies",
            params={'session_id': session_id}
        )
        
        if portfolio_response.status_code == 200 and strategies_response.status_code == 200:
            portfolio_data = portfolio_response.json()
            strategies_data = strategies_response.json()
            
            # Generate HTML report
            report_html = generate_html_report(portfolio_data, strategies_data['strategies'])
            
            with open(output, 'w') as f:
                f.write(report_html)
                
            click.echo(f"Report generated: {output}")
            
        else:
            click.echo(f"Error: {portfolio_response.text} {strategies_response.text}", err=True)
            
    except requests.exceptions.ConnectionError:
        click.echo("Error: Could not connect to API server", err=True)

def generate_html_report(portfolio_data: Dict, strategies: List[Dict]) -> str:
    """Generate HTML portfolio report"""
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Portfolio Report - {datetime.now().strftime('%Y-%m-%d')}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .section {{ margin: 20px 0; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h1>Portfolio Report</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <div class="section">
            <h2>Portfolio Summary</h2>
            <p>Total Value: <strong>${portfolio_data['total_value']:,.2f}</strong></p>
            <p>Cash: <strong>${portfolio_data['cash']:,.2f}</strong></p>
        </div>
        
        <div class="section">
            <h2>Positions</h2>
            <table>
                <tr><th>Asset</th><th>Quantity</th><th>Value</th><th>Weight</th></tr>
                {"".join(f"<tr><td>{asset}</td><td>{pos['quantity']}</td><td>${pos['value']:,.2f}</td><td>{portfolio_data['weights'].get(asset, 0):.1%}</td></tr>" 
                for asset, pos in portfolio_data['positions'].items())}
            </table>
        </div>
        
        <div class="section">
            <h2>Performance</h2>
            <table>
                <tr><th>Period</th><th>Return</th></tr>
                {"".join(f"<tr><td>{period}</td><td>{perf:.2%}</td></tr>" 
                for period, perf in portfolio_data['performance'].items())}
            </table>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    cli()