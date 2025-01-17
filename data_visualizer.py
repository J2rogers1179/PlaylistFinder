import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

class PlaylistVisualizer:
    @staticmethod
    def create_follower_distribution(df):
        """Create follower distribution chart"""
        fig = px.histogram(
            df,
            x="follower_count",
            nbins=50,
            title="Follower Distribution",
            labels={"follower_count": "Follower Count", "count": "Number of Playlists"}
        )
        return fig

    @staticmethod
    def create_track_count_scatter(df):
        """Create scatter plot of followers vs track count"""
        fig = px.scatter(
            df,
            x="track_count",
            y="follower_count",
            title="Followers vs Track Count",
            labels={
                "follower_count": "Follower Count",
                "track_count": "Track Count"
            }
            # Removed trendline="ols" since we don't have statsmodels
        )
        return fig

    @staticmethod
    def create_weekly_trends(df):
        """Create weekly growth trends"""
        # Convert last_updated to datetime if it's not already
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        df['week'] = df['last_updated'].dt.isocalendar().week
        
        weekly_stats = df.groupby('week').agg({
            'follower_count': 'mean',
            'id': 'count'
        }).reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=weekly_stats['week'],
            y=weekly_stats['follower_count'],
            name='Avg Followers',
            mode='lines+markers'
        ))
        fig.update_layout(title="Weekly Follower Trends")
        return fig

    @staticmethod
    def create_email_presence_pie(df):
        """Create pie chart of playlists with/without contact info"""
        has_email = df['email'].notna().sum()
        no_email = df['email'].isna().sum()
        
        fig = px.pie(
            values=[has_email, no_email],
            names=['Has Email', 'No Email'],
            title="Contact Information Availability"
        )
        return fig

    @staticmethod
    def create_curator_rankings(df, top_n=10):
        """Create bar chart of top curators by total followers"""
        curator_stats = df.groupby('curator_name').agg({
            'follower_count': 'sum',
            'playlist_name': 'count'
        }).reset_index()
        
        curator_stats = curator_stats.sort_values('follower_count', ascending=False).head(top_n)
        
        fig = px.bar(
            curator_stats,
            x='curator_name',
            y='follower_count',
            title=f'Top {top_n} Curators by Total Followers',
            labels={
                'curator_name': 'Curator',
                'follower_count': 'Total Followers'
            }
        )
        fig.update_layout(xaxis_tickangle=-45)
        return fig

    @staticmethod
    def create_engagement_scatter(df):
        """Create scatter plot of followers vs engagement rate"""
        # Add a small value to prevent division by zero
        df['engagement_rate'] = df['follower_count'] / (df['track_count'] + 1)
        
        fig = px.scatter(
            df,
            x='track_count',
            y='engagement_rate',
            title='Playlist Engagement Analysis',
            labels={
                'track_count': 'Number of Tracks',
                'engagement_rate': 'Followers per Track'
            },
            hover_data=['playlist_name', 'curator_name']
        )
        return fig

    @staticmethod
    def create_playlist_growth_heatmap(df):
        """Create heatmap of playlist growth over time"""
        # Convert last_updated to datetime if it's not already
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        df['month'] = df['last_updated'].dt.month
        df['day'] = df['last_updated'].dt.day
        
        # Create the pivot table for the heatmap
        growth_matrix = df.pivot_table(
            values='follower_count',
            index='day',
            columns='month',
            aggfunc='mean'
        ).fillna(0)
        
        fig = go.Figure(data=go.Heatmap(
            z=growth_matrix.values,
            x=growth_matrix.columns,
            y=growth_matrix.index,
            colorscale='Viridis'
        ))
        
        fig.update_layout(
            title='Playlist Growth Heatmap',
            xaxis_title='Month',
            yaxis_title='Day'
        )
        return fig

    def create_dashboard(self, df):
        """Create a complete dashboard with all visualizations"""
        return {
            'follower_dist': self.create_follower_distribution(df),
            'track_scatter': self.create_track_count_scatter(df),
            'weekly_trends': self.create_weekly_trends(df),
            'email_pie': self.create_email_presence_pie(df),
            'curator_rankings': self.create_curator_rankings(df),
            'engagement_scatter': self.create_engagement_scatter(df),
            'growth_heatmap': self.create_playlist_growth_heatmap(df)
        }