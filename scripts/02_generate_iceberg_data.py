#!/usr/bin/env python3
"""
ICEBERG CLD BCDR DEMO
Script: 02_generate_iceberg_data.py
Purpose: Generate sample advertising data for the Iceberg demo

This script generates realistic advertising data including:
- Campaigns: Marketing campaign definitions
- Impressions: Ad impression events
- Clicks: Click-through events
- Conversions: Purchase/conversion events

The data maintains referential integrity across all tables.

Usage:
    python 02_generate_iceberg_data.py --output-dir ./data --num-campaigns 50

Author: Snowflake Demo Team
"""

import os
import sys
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import click
import pandas as pd
from faker import Faker
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

# Initialize Faker and console
fake = Faker()
console = Console()

# Seed for reproducibility
SEED = 42
random.seed(SEED)
Faker.seed(SEED)

# Constants for data generation
CHANNELS = ['display', 'search', 'social', 'video', 'native', 'email', 'affiliate']
DEVICE_TYPES = ['desktop', 'mobile', 'tablet', 'connected_tv', 'other']
GEO_REGIONS = ['US-EAST', 'US-WEST', 'US-CENTRAL', 'CANADA', 'UK', 'EU-WEST', 'EU-CENTRAL', 'APAC']
AD_FORMATS = ['banner', 'interstitial', 'video_preroll', 'video_midroll', 'native_feed', 'carousel']
CONVERSION_TYPES = ['purchase', 'signup', 'download', 'subscription', 'lead_form']
CAMPAIGN_STATUS = ['active', 'paused', 'completed', 'draft']


def generate_campaigns(num_campaigns: int, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """Generate campaign dimension data."""
    campaigns = []
    
    for i in range(num_campaigns):
        campaign_start = fake.date_between(start_date=start_date, end_date=end_date - timedelta(days=30))
        campaign_end = campaign_start + timedelta(days=random.randint(7, 90))
        
        campaign = {
            'campaign_id': f'CMP-{str(i+1).zfill(6)}',
            'campaign_name': f'{fake.catch_phrase()} Campaign',
            'channel': random.choice(CHANNELS),
            'ad_format': random.choice(AD_FORMATS),
            'budget_usd': round(random.uniform(1000, 500000), 2),
            'daily_budget_usd': round(random.uniform(100, 10000), 2),
            'target_cpa_usd': round(random.uniform(5, 100), 2),
            'target_region': random.choice(GEO_REGIONS),
            'status': random.choice(CAMPAIGN_STATUS),
            'start_date': campaign_start.isoformat(),
            'end_date': campaign_end.isoformat(),
            'advertiser_name': fake.company(),
            'advertiser_industry': fake.bs(),
            'created_at': fake.date_time_between(
                start_date=campaign_start - timedelta(days=7),
                end_date=campaign_start
            ).isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        campaigns.append(campaign)
    
    return pd.DataFrame(campaigns)


def generate_impressions(
    campaigns_df: pd.DataFrame,
    impressions_per_campaign: int,
    start_date: datetime,
    end_date: datetime
) -> pd.DataFrame:
    """Generate impression event data."""
    impressions = []
    impression_id = 0
    
    active_campaigns = campaigns_df[campaigns_df['status'].isin(['active', 'completed'])]
    
    for _, campaign in active_campaigns.iterrows():
        num_impressions = random.randint(
            impressions_per_campaign // 2,
            impressions_per_campaign * 2
        )
        
        campaign_start = datetime.fromisoformat(campaign['start_date'])
        campaign_end = min(datetime.fromisoformat(campaign['end_date']), end_date)
        
        for _ in range(num_impressions):
            impression_id += 1
            impression_ts = fake.date_time_between(
                start_date=campaign_start,
                end_date=campaign_end
            )
            
            impression = {
                'impression_id': f'IMP-{str(impression_id).zfill(10)}',
                'campaign_id': campaign['campaign_id'],
                'timestamp': impression_ts.isoformat(),
                'date_key': impression_ts.strftime('%Y-%m-%d'),
                'hour': impression_ts.hour,
                'device_type': random.choice(DEVICE_TYPES),
                'geo_region': campaign['target_region'] if random.random() > 0.3 else random.choice(GEO_REGIONS),
                'geo_country': fake.country_code(),
                'geo_city': fake.city(),
                'browser': random.choice(['Chrome', 'Safari', 'Firefox', 'Edge', 'Other']),
                'os': random.choice(['Windows', 'macOS', 'iOS', 'Android', 'Linux', 'Other']),
                'ad_position': random.choice(['above_fold', 'below_fold', 'sidebar', 'in_feed']),
                'viewable': random.random() > 0.2,
                'cost_usd': round(random.uniform(0.001, 0.05), 4),
                'publisher_id': f'PUB-{random.randint(1, 500):04d}',
                'placement_id': f'PLC-{random.randint(1, 2000):05d}'
            }
            impressions.append(impression)
    
    return pd.DataFrame(impressions)


def generate_clicks(impressions_df: pd.DataFrame, click_rate: float = 0.02) -> pd.DataFrame:
    """Generate click event data from impressions."""
    clicks = []
    click_id = 0
    
    # Sample impressions that resulted in clicks
    clicked_impressions = impressions_df.sample(frac=click_rate)
    
    for _, impression in clicked_impressions.iterrows():
        click_id += 1
        impression_ts = datetime.fromisoformat(impression['timestamp'])
        click_ts = impression_ts + timedelta(seconds=random.randint(1, 30))
        
        click = {
            'click_id': f'CLK-{str(click_id).zfill(10)}',
            'impression_id': impression['impression_id'],
            'campaign_id': impression['campaign_id'],
            'timestamp': click_ts.isoformat(),
            'date_key': click_ts.strftime('%Y-%m-%d'),
            'device_type': impression['device_type'],
            'geo_region': impression['geo_region'],
            'geo_country': impression['geo_country'],
            'landing_page_url': fake.url(),
            'referrer_url': fake.url() if random.random() > 0.3 else None,
            'time_on_site_seconds': random.randint(5, 600) if random.random() > 0.4 else random.randint(1, 5),
            'pages_viewed': random.randint(1, 15),
            'bounce': random.random() < 0.4,
            'cost_usd': round(random.uniform(0.10, 2.00), 4)
        }
        clicks.append(click)
    
    return pd.DataFrame(clicks)


def generate_conversions(
    clicks_df: pd.DataFrame,
    campaigns_df: pd.DataFrame,
    conversion_rate: float = 0.05
) -> pd.DataFrame:
    """Generate conversion event data from clicks."""
    conversions = []
    conversion_id = 0
    
    # Sample clicks that resulted in conversions
    converted_clicks = clicks_df.sample(frac=conversion_rate)
    
    # Create campaign lookup for CPA targets
    campaign_cpa = campaigns_df.set_index('campaign_id')['target_cpa_usd'].to_dict()
    
    for _, click in converted_clicks.iterrows():
        conversion_id += 1
        click_ts = datetime.fromisoformat(click['timestamp'])
        
        # Conversion can happen immediately or within attribution window
        conversion_delay = timedelta(
            minutes=random.randint(1, 60 * 24 * 7)  # Up to 7 days
        ) if random.random() > 0.3 else timedelta(minutes=random.randint(1, 30))
        
        conversion_ts = click_ts + conversion_delay
        
        # Revenue based on campaign CPA target
        target_cpa = campaign_cpa.get(click['campaign_id'], 50)
        revenue = round(target_cpa * random.uniform(0.5, 5.0), 2)
        
        conversion = {
            'conversion_id': f'CNV-{str(conversion_id).zfill(10)}',
            'click_id': click['click_id'],
            'impression_id': click['impression_id'],
            'campaign_id': click['campaign_id'],
            'timestamp': conversion_ts.isoformat(),
            'date_key': conversion_ts.strftime('%Y-%m-%d'),
            'conversion_type': random.choice(CONVERSION_TYPES),
            'revenue_usd': revenue,
            'quantity': random.randint(1, 5),
            'currency': 'USD',
            'order_id': f'ORD-{fake.uuid4()[:8].upper()}',
            'product_category': fake.ecommerce_category() if hasattr(fake, 'ecommerce_category') else random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Beauty']),
            'new_customer': random.random() > 0.6,
            'device_type': click['device_type'],
            'geo_region': click['geo_region'],
            'attribution_model': random.choice(['last_click', 'first_click', 'linear', 'time_decay'])
        }
        conversions.append(conversion)
    
    return pd.DataFrame(conversions)


@click.command()
@click.option(
    '--output-dir', '-o',
    type=click.Path(),
    default='./data',
    help='Output directory for generated CSV files'
)
@click.option(
    '--num-campaigns', '-c',
    type=int,
    default=50,
    help='Number of campaigns to generate'
)
@click.option(
    '--impressions-per-campaign', '-i',
    type=int,
    default=10000,
    help='Average impressions per campaign'
)
@click.option(
    '--click-rate', '-cr',
    type=float,
    default=0.02,
    help='Click-through rate (0.0 to 1.0)'
)
@click.option(
    '--conversion-rate', '-cvr',
    type=float,
    default=0.05,
    help='Conversion rate from clicks (0.0 to 1.0)'
)
@click.option(
    '--start-date', '-s',
    type=str,
    default=None,
    help='Start date for data (YYYY-MM-DD), defaults to 90 days ago'
)
@click.option(
    '--end-date', '-e',
    type=str,
    default=None,
    help='End date for data (YYYY-MM-DD), defaults to today'
)
@click.option(
    '--parquet', '-p',
    is_flag=True,
    default=False,
    help='Also output Parquet files (for direct Iceberg loading)'
)
def main(
    output_dir: str,
    num_campaigns: int,
    impressions_per_campaign: int,
    click_rate: float,
    conversion_rate: float,
    start_date: Optional[str],
    end_date: Optional[str],
    parquet: bool
):
    """Generate sample advertising data for Iceberg CLD BCDR demo."""
    
    console.print("\n[bold blue]═══════════════════════════════════════════════════════════════[/]")
    console.print("[bold blue]       ICEBERG CLD BCDR DEMO - Data Generator[/]")
    console.print("[bold blue]═══════════════════════════════════════════════════════════════[/]\n")
    
    # Parse dates
    end_dt = datetime.fromisoformat(end_date) if end_date else datetime.now()
    start_dt = datetime.fromisoformat(start_date) if start_date else end_dt - timedelta(days=90)
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    console.print(f"[cyan]Configuration:[/]")
    console.print(f"  • Output directory: {output_path.absolute()}")
    console.print(f"  • Campaigns: {num_campaigns}")
    console.print(f"  • Impressions/campaign: ~{impressions_per_campaign}")
    console.print(f"  • Click rate: {click_rate:.1%}")
    console.print(f"  • Conversion rate: {conversion_rate:.1%}")
    console.print(f"  • Date range: {start_dt.date()} to {end_dt.date()}\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        
        # Generate campaigns
        task = progress.add_task("[cyan]Generating campaigns...", total=None)
        campaigns_df = generate_campaigns(num_campaigns, start_dt, end_dt)
        progress.update(task, completed=True, description=f"[green]✓ Generated {len(campaigns_df):,} campaigns")
        
        # Generate impressions
        task = progress.add_task("[cyan]Generating impressions...", total=None)
        impressions_df = generate_impressions(campaigns_df, impressions_per_campaign, start_dt, end_dt)
        progress.update(task, completed=True, description=f"[green]✓ Generated {len(impressions_df):,} impressions")
        
        # Generate clicks
        task = progress.add_task("[cyan]Generating clicks...", total=None)
        clicks_df = generate_clicks(impressions_df, click_rate)
        progress.update(task, completed=True, description=f"[green]✓ Generated {len(clicks_df):,} clicks")
        
        # Generate conversions
        task = progress.add_task("[cyan]Generating conversions...", total=None)
        conversions_df = generate_conversions(clicks_df, campaigns_df, conversion_rate)
        progress.update(task, completed=True, description=f"[green]✓ Generated {len(conversions_df):,} conversions")
        
        # Save CSV files
        task = progress.add_task("[cyan]Saving CSV files...", total=None)
        campaigns_df.to_csv(output_path / 'campaigns.csv', index=False)
        impressions_df.to_csv(output_path / 'impressions.csv', index=False)
        clicks_df.to_csv(output_path / 'clicks.csv', index=False)
        conversions_df.to_csv(output_path / 'conversions.csv', index=False)
        progress.update(task, completed=True, description="[green]✓ Saved CSV files")
        
        # Optionally save Parquet files
        if parquet:
            task = progress.add_task("[cyan]Saving Parquet files...", total=None)
            campaigns_df.to_parquet(output_path / 'campaigns.parquet', index=False)
            impressions_df.to_parquet(output_path / 'impressions.parquet', index=False)
            clicks_df.to_parquet(output_path / 'clicks.parquet', index=False)
            conversions_df.to_parquet(output_path / 'conversions.parquet', index=False)
            progress.update(task, completed=True, description="[green]✓ Saved Parquet files")
    
    # Summary
    console.print("\n[bold green]═══════════════════════════════════════════════════════════════[/]")
    console.print("[bold green]                    Generation Complete![/]")
    console.print("[bold green]═══════════════════════════════════════════════════════════════[/]\n")
    
    console.print("[bold]Summary:[/]")
    console.print(f"  • Campaigns:   {len(campaigns_df):>10,}")
    console.print(f"  • Impressions: {len(impressions_df):>10,}")
    console.print(f"  • Clicks:      {len(clicks_df):>10,}")
    console.print(f"  • Conversions: {len(conversions_df):>10,}")
    
    total_revenue = conversions_df['revenue_usd'].sum()
    total_cost = impressions_df['cost_usd'].sum() + clicks_df['cost_usd'].sum()
    roas = total_revenue / total_cost if total_cost > 0 else 0
    
    console.print(f"\n[bold]Metrics:[/]")
    console.print(f"  • Total Revenue:  ${total_revenue:>12,.2f}")
    console.print(f"  • Total Ad Spend: ${total_cost:>12,.2f}")
    console.print(f"  • ROAS:           {roas:>12.2f}x")
    
    console.print(f"\n[cyan]Files saved to: {output_path.absolute()}[/]")
    console.print("\n[yellow]Next step:[/] Run 03_load_iceberg_aws.py to load data into Iceberg tables\n")


if __name__ == '__main__':
    main()

