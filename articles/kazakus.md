Since the release of Mean Streets of Gadgetzan last December, Kazakus has without doubt been one of my favorite cards to play. As a fan of Kabal/Reno control-style decks, the added variety from crafting Kazakus spells not only helps keep games fresh and exciting, but also strengthens the sense of player agency over the direction of the game.
However, choosing when to play Kazakus and selecting which spell options (either to win or purely for fun) can be challenging depending on the opponent’s class and which potion effect options are randomly presented. In some cases, playing Kazakus is a great way to get out of a bind, but at other times it can be hard to anticipate what the board state will be in the upcoming few turns.
For instance, imagine a game where you’ve managed to survive the initial rush against a Shaman/Pirate/Zoo deck, it’s turn six and you draw Kazakus, but now what? Do you play Kazakus now or wait? Which spell cost choice is best? Which potion effect options are optimal? What if the choice you wanted isn’t offered, then what should you pick?

As both a curious Hearthstone player and lover of "big data", the primary goal of the article you’re about to read is to shed some light on these questions using a data-driven approach. The following article is the result of a month of research and experimentation with Kazakus data and visualizations.


### Data collection

The data consists of 52 days of replays (standard ladder games only, ranks 20 through legend) uploaded to HSReplay.net from January 27, 2017 to March 19, 2017. The dataset contains 3.81 million unique Mage, Priest, and Warlock games where Kazakus is present in the friendly player’s deck list. The histograms below presents some basic summary statistics on how many friendly Kazakus potions are created per game:

![](media/image2.png){width="6.5in" height="1.9715277777777778in"}

Some initial observations:

- Mage is played twice as much as Priest, & Warlock is played three times as much as Priest
- In over 55% of games, Kazakus is either never drawn or never played (i.e., 0 potions created)
- Two potions are created in the same game only 7-10% of the time (typically due to Brann)
- In less than 1% of games are 3 or more Kazakus potions created (typically due to cards like Brann, Drakonid Operative, Kabal Courier, & Maniac Soulcaster)

Based on the low frequency of multi-Kazakus potion games and also because Brann rotates soon, games with more than one potion created will be omitted from the rest of the article.


### Win rates per Potion Effect

Let's look at the value of each potion option against each enemy class. Specifically, their average win rate.
Here we look at each Potion Effect individually (as opposed to pairs), because it more closely mirrors how Kazakus potions are created in a live game, where the player has no knowledge of the upcoming choices.

The following figure explains each of the elements of the bar charts. Potion effects are color coded using the same scheme across charts to facilitate comparisons. The number of observed games can also be used to gain a sense of how popular each potion-effect is relative to each enemy class. For instance, we can see below vs. Druid that *Summon a 5/5 demon* is chosen almost twice as often as *Give your minions +4 health*.

![](media/image3.png){width="6.5in" height="2.272222222222222in"}

#### Results

There is a lot of useful insight within the following charts and we’ll provide you with a few teasers to get you started. For instance, there is only a single 1-cost option vs Druid/Rogue/Warrior that is likely to gives a positive win-rate (i.e. greater than 50% chance). For 5-cost potions vs Warrior, the highest win-rate option is the second to least picked! Also not surprisingly, *Freezing* *random enemy minions* is almost always the worst option against all classes and for all potion costs! Conversely, *giving your minions +2/4/6 Health* appears as the top option in almost all cases. Don’t forget to let us know in the comments what other insights jump out to you.

![](media/image4.png){width="1.90625in" height="1.3641601049868766in"} ![](media/image5.png){width="1.8814621609798776in" height="1.3680555555555556in"} ![](media/image6.png){width="1.8958333333333333in" height="1.389870953630796in"}

![](media/image7.png){width="1.90625in" height="1.38834864391951in"} ![](media/image8.png){width="1.9202099737532807in" height="1.3888888888888888in"} ![](media/image9.png){width="1.8993055555555556in" height="1.3944652230971128in"}

![](media/image10.png){width="2.101482939632546in" height="1.5034722222222223in"} ![](media/image11.png){width="2.051938976377953in" height="1.5138888888888888in"} ![](media/image12.png){width="2.111111111111111in" height="1.5136909448818898in"}


### To play or not to play Kazakus?

The big question to address here is which *cost* option to pick when Kazakus is played, and whether it makes sense to even play Kazakus on a given turn.

Let's look at how win rate is impacted when playing Kazakus vs. not, for each turn. Here, the win rates when playing Kazakus are computed relative to games where players had Kazakus in hand on the same turn and chose not to play it; this ensures that the heatmaps are not biased to changes in overall win rates due to the nature of the game's progress (i.e., for control/Kazakus decks, typically the more turns you survive, the better your chances of winning).

**How to read the heatmaps**: Each cell in the heatmap shows the increase/decrease in win-rate when choosing to play Kazakus on that given turn compared to the win-rate when players chose not to play Kazakus (i.e., Kazakus was kept in hand). The following figure provides further clarification how to interpret each cell:

![](media/image13.png){width="6.0669149168853895in" height="2.2621292650918634in"}

Also worth noting, cells with missing data occur when sample sizes are too low to generate a reliable value.

#### Results

Once again, we provide a few teasers to help understand some of the possible insights that can be gained from this data. For instance, in some matchups the most effective use of Kazakus is to select a 10-cost spell (e.g., Priest vs. Priest where it’s a game of value). In some cases, coining-out a turn 3 Kazakus and selecting a 10-cost spell (e.g., Priest vs. Shaman) is a good option, possibly because the opposing player may be inclined to play more conservatively around lower-cost potion options. In other cases, coining-out a turn 3 Kazakus and selecting a 10-cost spell (e.g., Warlock vs. Druid) is a terrible idea. Don’t forget to let us know in the comments what other insights jump out to you.


### Appendix

Providing confidence intervals on each bar is important because otherwise incorrect conclusions could be made from the visualizations. Confidence intervals are a form of error-bar (think standard deviation), but differ in that an underlying statistical test has been carried out (i.e., [ANOVA](https://en.wikipedia.org/wiki/Analysis_of_variance). In this article, I use a 75% confidence interval[^1], meaning that there is 75% certainty (i.e., confidence) that that true mean of the reported data lies within the range of the confidence interval. To illustrate, consider the following example (*left*) where we might be tempted to conclude that *Give your minions +2 Health* is the better option, but with confidence intervals shown (*left*), it’s likely (with 75% certainty) that *Give your minions +2 Health* is neither better nor worse than *Summon a 2/2 demon* because both intervals overlap:

![](media/image18.png){width="6.5in" height="0.7694444444444445in"}

In the next example however, it is safer to conclude (with 75% certainty) that *Give your minions +2 Health* option is better than *Deal 3 damage* since the confidence intervals do not overlap:

![](media/image19.png){width="2.8988440507436573in" height="0.6353630796150481in"}

Putting it all together, we can see in this last example the two options: *Give your minions +4 health* and *Deal 4 damage to all minions* are both equivalent top picks; and then the four options: *Summon a 5/5 demon*, *Summon 2 friendly minions that died this game*, *Draw 2 cards*, and *Add 2 random Demons to your hand,* are each equally tied as the second best options:

![](media/image20.png){width="2.9591076115485566in" height="1.4061548556430445in"}

In order to compute confidence intervals from in this dataset, replays were first segmented into 2 day (48 hour) periods. Next, win-rates are computed over each time segment (there are a total of 52days / 2 = 26 segments). Lastly, confidence intervals are computed for each potion option, along with average win-rates across the 26 segments. In general you will notice that infrequently selected options typically have larger confidence intervals (thus there is less confidence as to the true value of this option), which occurs most often against the Paladin and Hunter classes since these classes are currently less popular.

[^1]: Academia typically requires a *p*-value of .05 (i.e., 95% confidence intervals), however for practical purposes and in my experience in commercial applications, .25 is a commonly used value (i.e., 75 confidence intervals).


### Acknowledgements

This article was written by Dereck Toker, currently a Ph.D candidate in machine learning and data visualization and a consultant in the video game and patent industries.
You can reach Dereck [on Discord](https://discord.gg/hearthsim) as **Dereck Toker#1938**.

Data pre-processing, feature generation, and data visualization was carried out using R, and used the libraries: data.table, Rmisc, ggplot2, grid, & gridExtra.

![](media/image14.png){width="2.0225765529308837in" height="1.4861111111111112in"} ![](media/image15.png){width="2.070647419072616in" height="1.4861111111111112in"} ![](media/image16.png){width="2.048611111111111in" height="1.4970625546806648in"}
![](media/image17.png){width="0.8159722222222222in" height="0.8101443569553806in"}
