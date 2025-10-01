import discord
from discord import app_commands
from discord.ext import commands
import os
from typing import Dict, Any, List

from voidsdatastore import get_value, update_value

# GUILD for slash command registration
GUILD_ID = 1411904165814206516

intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)

# ---------------- Datastore Helpers ---------------- #

def _lb_key(lb_name: str) -> str:
    return f"leaderboard:{lb_name}"

def load_leaderboard(guild_id: int, lb_name: str) -> Dict[str, Any] | None:
    try:
        return get_value(str(guild_id), _lb_key(lb_name))
    except Exception:
        return None

def save_leaderboard(guild_id: int, lb_name: str, data: Dict[str, Any]) -> None:
    update_value(str(guild_id), _lb_key(lb_name), data)

def delete_leaderboard(guild_id: int, lb_name: str) -> None:
    # Remove leaderboard key
    update_value(str(guild_id), _lb_key(lb_name), None)

def list_leaderboards(guild_id: int) -> List[str]:
    try:
        data = get_value(str(guild_id), "leaderboards_index")
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []

def add_leaderboard_to_index(guild_id: int, lb_name: str):
    lbs = list_leaderboards(guild_id)
    if lb_name not in lbs:
        lbs.append(lb_name)
        update_value(str(guild_id), "leaderboards_index", lbs)

def remove_leaderboard_from_index(guild_id: int, lb_name: str):
    lbs = list_leaderboards(guild_id)
    if lb_name in lbs:
        lbs.remove(lb_name)
        update_value(str(guild_id), "leaderboards_index", lbs)

# ---------------- Formatting ---------------- #

def format_leaderboard(name: str, data: dict):
    scores = sorted(data["scores"].items(), key=lambda x: x[1], reverse=True)
    prefix = data.get("prefix", "")
    suffix = data.get("suffix", "")

    embed = discord.Embed(
        title=f"🏆 Leaderboard: {name}",
        color=discord.Color.gold()
    )

    if not scores:
        embed.description = "No entries yet!"
        return embed

    places = ["🥇 1st", "🥈 2nd", "🥉 3rd"]
    for idx, (user_id, score) in enumerate(scores[:10]):
        try:
            user_display = f"<@{int(user_id)}>"
        except Exception:
            user_display = str(user_id)

        if idx < 3:
            place = places[idx]
        else:
            place = f"{idx+1}."
        embed.add_field(
            name=place,
            value=f"{prefix}{score}{suffix} — {user_display}",
            inline=False
        )

    return embed

# ---------------- Slash Commands ---------------- #

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"✅ Synced {len(synced)} commands for guild {GUILD_ID}")
    except Exception as e:
        print(f"❌ Sync error: {e}")

# ---------------- Commands ---------------- #

# Create leaderboard
@bot.tree.command(name="create_lb", description="Create a leaderboard", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(name="Leaderboard name", position="Prefix or suffix", symbol="Optional symbol (max 7 chars)")
async def create_lb(interaction: discord.Interaction, name: str, position: str = "prefix", symbol: str = ""):
    await interaction.response.defer()
    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send("⚠️ Admins only.", ephemeral=True)
        return

    if len(symbol) > 7:
        await interaction.followup.send("⚠️ Prefix/suffix cannot exceed 7 characters.", ephemeral=True)
        return

    if load_leaderboard(interaction.guild_id, name):
        await interaction.followup.send("⚠️ Leaderboard already exists.", ephemeral=True)
        return

    data = {"prefix": "", "suffix": "", "scores": {}}
    if symbol:
        if position.lower() == "prefix":
            data["prefix"] = symbol
        elif position.lower() == "suffix":
            data["suffix"] = symbol

    save_leaderboard(interaction.guild_id, name, data)
    add_leaderboard_to_index(interaction.guild_id, name)

    await interaction.followup.send(f"✅ Created leaderboard **{name}**")

# Set or update score
@bot.tree.command(name="set_score", description="Set a member's score", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(lb_name="Leaderboard name", member="Member to update", score="Score value")
async def set_score(interaction: discord.Interaction, lb_name: str, member: discord.Member, score: int):
    await interaction.response.defer()
    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send("⚠️ Admins only.", ephemeral=True)
        return

    data = load_leaderboard(interaction.guild_id, lb_name)
    if not data:
        await interaction.followup.send("⚠️ Leaderboard not found.", ephemeral=True)
        return

    # --- FIX STARTS HERE ---
    if "scores" not in data or not isinstance(data.get("scores"), dict):
        # Re-initialize the 'scores' key if it's missing or not a dictionary
        data["scores"] = {}
    # --- FIX ENDS HERE ---

    data["scores"][str(member.id)] = score
    save_leaderboard(interaction.guild_id, lb_name, data)
    await interaction.followup.send(f"✅ Set {member.mention}'s score to {score} on **{lb_name}**")
# View leaderboard
@bot.tree.command(name="leaderboard", description="View a leaderboard", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(lb_name="Leaderboard name")
async def leaderboard(interaction: discord.Interaction, lb_name: str):
    await interaction.response.defer()
    data = load_leaderboard(interaction.guild_id, lb_name)
    print(data)
    if not data:
        await interaction.followup.send("⚠️ Leaderboard not found.", ephemeral=True)
        return

    embed = format_leaderboard(lb_name, data)
    await interaction.followup.send(embed=embed)

# List leaderboards
@bot.tree.command(name="list_lbs", description="List all leaderboards", guild=discord.Object(id=GUILD_ID))
async def list_lbs(interaction: discord.Interaction):
    await interaction.response.defer()
    lbs = list_leaderboards(interaction.guild_id)
    if not lbs:
        await interaction.followup.send("⚠️ No leaderboards available.")
        return

    embed = discord.Embed(
        title="📜 Available Leaderboards",
        description="\n".join([f"• {lb}" for lb in lbs]),
        color=discord.Color.blue()
    )
    await interaction.followup.send(embed=embed)

# Delete leaderboard
@bot.tree.command(name="delete_lb", description="Delete a leaderboard", guild=discord.Object(id=GUILD_ID))
@app_commands.describe(lb_name="Leaderboard name")
async def delete_lb(interaction: discord.Interaction, lb_name: str):
    await interaction.response.defer()
    if not interaction.user.guild_permissions.administrator:
        await interaction.followup.send("⚠️ Admins only.", ephemeral=True)
        return

    data = load_leaderboard(interaction.guild_id, lb_name)
    if not data:
        await interaction.followup.send("⚠️ Leaderboard not found.", ephemeral=True)
        return

    delete_leaderboard(interaction.guild_id, lb_name)
    remove_leaderboard_from_index(interaction.guild_id, lb_name)
    await interaction.followup.send(f"✅ Deleted leaderboard **{lb_name}**")

# ---------------- Run ---------------- #

if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("❌ DISCORD_TOKEN environment variable not set.")
    bot.run(token)