#!/usr/bin/env python3
"""
Generate performance improvement chart for Twitter thread
Shows before/after comparison of Arc write optimizations
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Arc Write Performance Optimization Results', fontsize=18, fontweight='bold', y=0.98)

# Colors
color_before = '#e74c3c'  # Red
color_after = '#2ecc71'   # Green
color_improvement = '#3498db'  # Blue

# ============================================================================
# Chart 1: Throughput Comparison
# ============================================================================
ax1 = axes[0]

throughput_data = {
    'Before': 1.95,
    'After': 2.01
}

bars = ax1.bar(throughput_data.keys(), throughput_data.values(),
               color=[color_before, color_after], alpha=0.8, edgecolor='black', linewidth=2)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{height:.2f}M',
             ha='center', va='bottom', fontsize=14, fontweight='bold')

# Add improvement annotation
improvement_pct = ((2.01 - 1.95) / 1.95) * 100
ax1.annotate(f'+{improvement_pct:.1f}%',
             xy=(1, 2.01), xytext=(1, 2.08),
             ha='center', fontsize=14, fontweight='bold',
             color=color_improvement,
             bbox=dict(boxstyle='round,pad=0.5', facecolor='white', edgecolor=color_improvement, linewidth=2))

ax1.set_ylabel('Records per Second (Millions)', fontsize=12, fontweight='bold')
ax1.set_title('Throughput: +3.1%', fontsize=14, fontweight='bold', pad=15)
ax1.set_ylim(0, 2.2)
ax1.grid(axis='y', alpha=0.3)

# ============================================================================
# Chart 2: Latency Improvements (Lower is Better)
# ============================================================================
ax2 = axes[1]

latency_metrics = ['p50\n(Median)', 'p95\n(95th pct)', 'p99\n(99th pct)']
before_latency = [18.21, 184.60, 395.12]
after_latency = [16.62, 147.12, 317.53]

x = np.arange(len(latency_metrics))
width = 0.35

bars1 = ax2.bar(x - width/2, before_latency, width, label='Before',
                color=color_before, alpha=0.8, edgecolor='black', linewidth=2)
bars2 = ax2.bar(x + width/2, after_latency, width, label='After',
                color=color_after, alpha=0.8, edgecolor='black', linewidth=2)

# Add value labels on bars
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                 f'{height:.1f}ms',
                 ha='center', va='bottom', fontsize=10, fontweight='bold')

# Add improvement percentages
improvements = [
    ((18.21 - 16.62) / 18.21) * 100,
    ((184.60 - 147.12) / 184.60) * 100,
    ((395.12 - 317.53) / 395.12) * 100
]

for i, (imp, metric) in enumerate(zip(improvements, latency_metrics)):
    ax2.annotate(f'-{imp:.1f}%',
                 xy=(i, max(before_latency[i], after_latency[i])),
                 xytext=(i, max(before_latency[i], after_latency[i]) + 50),
                 ha='center', fontsize=11, fontweight='bold',
                 color=color_improvement,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                          edgecolor=color_improvement, linewidth=2),
                 arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0',
                               color=color_improvement, lw=2))

ax2.set_ylabel('Latency (milliseconds)', fontsize=12, fontweight='bold')
ax2.set_title('Latency: Up to -20% (Lower is Better)', fontsize=14, fontweight='bold', pad=15)
ax2.set_xticks(x)
ax2.set_xticklabels(latency_metrics, fontsize=11)
ax2.legend(fontsize=11, loc='upper left')
ax2.grid(axis='y', alpha=0.3)

# ============================================================================
# Footer
# ============================================================================
fig.text(0.5, 0.02,
         'Benchmark: Apple M3 Max (14 cores), 400 workers, 30s sustained load, MessagePack binary protocol',
         ha='center', fontsize=10, style='italic', color='gray')

fig.text(0.5, -0.01,
         'Optimizations: MessagePack streaming decoder + Columnar Polars construction',
         ha='center', fontsize=10, fontweight='bold', color='#34495e')

plt.tight_layout(rect=[0, 0.04, 1, 0.96])

# Save
output_path = '/Users/nacho/dev/basekick-labs/arc/assets/performance_improvement.png'
plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"✅ Chart saved to: {output_path}")

# Also save a Twitter-optimized version (16:9 aspect ratio)
fig2, axes2 = plt.subplots(1, 2, figsize=(16, 9))
fig2.suptitle('Arc Write Performance: 2.01M RPS 🚀', fontsize=24, fontweight='bold', y=0.96)

# Recreate charts with larger fonts for Twitter
# Chart 1: Throughput
ax1_tw = axes2[0]
bars = ax1_tw.bar(throughput_data.keys(), throughput_data.values(),
                  color=[color_before, color_after], alpha=0.8, edgecolor='black', linewidth=3)
for bar in bars:
    height = bar.get_height()
    ax1_tw.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}M',
                ha='center', va='bottom', fontsize=20, fontweight='bold')

ax1_tw.annotate(f'+{improvement_pct:.1f}%',
                xy=(1, 2.01), xytext=(1, 2.08),
                ha='center', fontsize=20, fontweight='bold',
                color=color_improvement,
                bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                         edgecolor=color_improvement, linewidth=3))

ax1_tw.set_ylabel('Records/sec (Millions)', fontsize=16, fontweight='bold')
ax1_tw.set_title('Throughput: +3.1%', fontsize=20, fontweight='bold', pad=20)
ax1_tw.set_ylim(0, 2.2)
ax1_tw.grid(axis='y', alpha=0.3)
ax1_tw.tick_params(labelsize=14)

# Chart 2: Latency
ax2_tw = axes2[1]
bars1 = ax2_tw.bar(x - width/2, before_latency, width, label='Before',
                   color=color_before, alpha=0.8, edgecolor='black', linewidth=3)
bars2 = ax2_tw.bar(x + width/2, after_latency, width, label='After',
                   color=color_after, alpha=0.8, edgecolor='black', linewidth=3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax2_tw.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.0f}ms',
                    ha='center', va='bottom', fontsize=14, fontweight='bold')

for i, (imp, metric) in enumerate(zip(improvements, latency_metrics)):
    ax2_tw.annotate(f'-{imp:.1f}%',
                    xy=(i, max(before_latency[i], after_latency[i])),
                    xytext=(i, max(before_latency[i], after_latency[i]) + 50),
                    ha='center', fontsize=16, fontweight='bold',
                    color=color_improvement,
                    bbox=dict(boxstyle='round,pad=0.5', facecolor='white',
                             edgecolor=color_improvement, linewidth=3),
                    arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0',
                                  color=color_improvement, lw=3))

ax2_tw.set_ylabel('Latency (milliseconds)', fontsize=16, fontweight='bold')
ax2_tw.set_title('Latency: Up to -20%', fontsize=20, fontweight='bold', pad=20)
ax2_tw.set_xticks(x)
ax2_tw.set_xticklabels(latency_metrics, fontsize=14)
ax2_tw.legend(fontsize=14, loc='upper left')
ax2_tw.grid(axis='y', alpha=0.3)
ax2_tw.tick_params(labelsize=14)

fig2.text(0.5, 0.03,
          '61M records • 30s test • 400 workers • 100% success • MessagePack binary',
          ha='center', fontsize=14, fontweight='bold', color='#34495e')

plt.tight_layout(rect=[0, 0.06, 1, 0.94])

output_path_tw = '/Users/nacho/dev/basekick-labs/arc/assets/performance_improvement_twitter.png'
plt.savefig(output_path_tw, dpi=150, bbox_inches='tight', facecolor='white')
print(f"✅ Twitter chart saved to: {output_path_tw}")

print("\n📊 Both charts generated successfully!")
print("\nUse performance_improvement_twitter.png for tweet 5")
