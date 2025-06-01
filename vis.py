import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("data.csv")

# convert ns/ops -> ms
df["direct_upsert_ms"] = df["direct_upsert_op"] / 1e6
df["staging_copy_ms"] = df["staging_copy_op"] / 1e6

plt.figure(figsize=(6, 4))
ax = plt.gca()
plt.plot(
    df["row_count"],
    df["direct_upsert_ms"],
    color="k",
    linestyle="-",
    marker="o",
    linewidth=1.5,
    markersize=6,
    label="Direct Upsert",
)
plt.plot(
    df["row_count"],
    df["staging_copy_ms"],
    color="k",
    linestyle="--",
    marker="s",
    linewidth=1.5,
    markersize=6,
    label="Staging + COPY",
)

for spine in ax.spines.values():
    spine.set_color("k")
    spine.set_linewidth(1.0)

ax.tick_params(colors="k", labelsize=8)
ax.grid(True, linestyle=":", linewidth=0.7, color="grey", alpha=0.5)

legend = ax.legend(frameon=False, fontsize=8)
for text in legend.get_texts():
    text.set_color("k")

plt.title("Benchmark: Direct Upsert vs Staging COPY Upsert", fontsize=10, pad=8)
plt.xlabel("Number of Rows", fontsize=8)
plt.ylabel("Time per Operation (ms)", fontsize=8)
plt.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)
plt.legend(loc="upper left")

plt.tight_layout()
# plt.show()
plt.savefig("comparison.png")
