import discord
from discord import app_commands
from discord.ext import commands
import os
from typing import Dict, Any, List

from voidsdatastore import get_value, update_value

# GUILD for slash command registration
GUILD_ID = 1429301285613600931

intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)

def _lb_key(lb_name: str) -> str:
    return f"leaderboard:{lb_name}"


def load_key(guild_id: int, lb_name: str) -> Dict[str, Any] | None:
    try:
        val = get_value(str(guild_id), _lb_key(lb_name))
        if "Type" in val["message"] and val["message"]["Type"] == "None":
            return None
        else:
            return val["message"]
    except Exception:
        return None


def save_key(guild_id: int, lb_name: str, data: Dict[str, Any]) -> None:
    update_value(str(guild_id), _lb_key(lb_name), data)


def delete_key(guild_id: int, lb_name: str) -> None:
    update_value(str(guild_id), _lb_key(lb_name), None)


def exchange_currency(from_user: discord.Member, to_user: discord.Member, amount: int, taxrate: float):
    userdata = load_key(GUILD_ID, str(from_user.id))
    userdata2 = load_key(GUILD_ID, str(to_user.id))

    taxed = round(amount*taxrate)

    if userdata["Balance"] < amount:
        return False
    userdata["Balance"] -= amount
    amount -= taxed
    userdata2["Balance"] += amount

    save_key(GUILD_ID, str(from_user.id), userdata)
    save_key(GUILD_ID, str(to_user.id), userdata2)

    royalty = load_key(GUILD_ID, str(1105981751647539241))
    royalty["Balance"] += taxed

    save_key(GUILD_ID, str(1105981751647539241), royalty)
    return True


@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        print(f"✅ Synced {len(synced)} commands for guild {GUILD_ID}")
        guild = bot.get_guild(GUILD_ID)
        role = guild.get_role(1429329855815745537)
        for user in guild.members:
            if not user.bot and not role in user.roles:
                await user.add_roles(role)
    except Exception as e:
        print(f"❌ Sync error: {e}")


@bot.event
async def on_member_join(member: discord.Member):
    guild = bot.get_guild(GUILD_ID)
    role = guild.get_role(1429329855815745537)
    await member.add_roles(role)

async def datacheck(interaction: discord.Interaction):
    userdata = load_key(interaction.guild.id, str(interaction.user.id))
    if userdata is None:
        userdata = {}
    required = {
        "Balance": 0
    }
    doit = False
    for key, value in required.items():
        if key not in userdata:
            doit = True
            userdata[key] = value

    if doit:
        save_key(interaction.guild.id, str(interaction.user.id), userdata)

    return True


@bot.tree.command(name="identity", description="Check yourself", guild=discord.Object(id=GUILD_ID))
@app_commands.guild_install
@app_commands.check(datacheck)
async def balance(interaction: discord.Interaction):
    await interaction.response.defer()

    userdata = load_key(interaction.guild.id, str(interaction.user.id))

    embed = discord.Embed(title=f"{interaction.user.name}'s Identity", description="",color=discord.Color.green())
    embed.set_author(name=f"{interaction.user.name}", icon_url=interaction.user.avatar.url)
    embed.add_field(name="Bits", value=userdata["Balance"])
    enforcement = [1429302329022349514,1429302169517031445,1429302091595255938]
    royalty = [1429301895306022932,1429301761687945216]
    role = interaction.user.top_role.id

    userclass = "Basic/Civilian"
    if role in enforcement:
        userclass = "Enforcement"
    if role in royalty:
        userclass = "Royalty"

    embed.add_field(name="Class", value=userclass)

    await interaction.followup.send(embed=embed)


@bot.tree.command(name="exchange", description="Exchange someone bits", guild=discord.Object(id=GUILD_ID))
@app_commands.guild_install
@app_commands.check(datacheck)
async def exchange(interaction: discord.Interaction, user: discord.Member, amount: int):
    await interaction.response.defer()

    channel = interaction.guild.get_channel(1429334134416998450)
    if amount > 200:
        taxrate = 0.2
        enforcement = [1429302329022349514, 1429302169517031445, 1429302091595255938]
        role = user.top_role.id

        if role in enforcement:
            taxrate = 0.1
    else:
        taxrate = 0

    if exchange_currency(interaction.user, user, amount, taxrate):
        embed = discord.Embed(title=f"{interaction.user.name}'s Exchange", description="",color=discord.Color.green())
        embed.set_author(name=f"{interaction.user.name}", icon_url=interaction.user.avatar.url)
        embed.add_field(name="Process Result", value=f"Successfully exchanged {amount-round((amount*taxrate))} ({round(amount*taxrate)} taxed) bits to {user.name}")

        await interaction.followup.send(embed=embed)
        await channel.send(embed=embed)
    else:
        embed = discord.Embed(title=f"{interaction.user.name}'s Exchange", description="", color=discord.Color.red())
        embed.set_author(name=f"{interaction.user.name}", icon_url=interaction.user.avatar.url)
        embed.add_field(name="Process Result", value=f"Unable to exchange, funds insufficient.")

        await interaction.followup.send(embed=embed)


if __name__ == "__main__":
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("❌ DISCORD_TOKEN environment variable not set.")
    bot.run(token)