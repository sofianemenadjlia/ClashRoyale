import deck from './deck.js';

const { Deck, nameToUrl, nameToId, idToCard } = deck;

const maxK = 20;
const dataPath = 'deck-stats/'

function handleTimeFilter() {
    const timeFilter = document.getElementById('timeFilter').value;
    const monthFilter = document.getElementById('monthFilter');
    const weekFilter = document.getElementById('weekFilter');

    if (timeFilter === 'month') {
        monthFilter.style.display = 'block';
        weekFilter.style.display = 'none';
    } else if (timeFilter === 'week') {
        monthFilter.style.display = 'none';
        weekFilter.style.display = 'block';
    } else {
        monthFilter.style.display = 'none';
        weekFilter.style.display = 'none';
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const timeFilterElement = document.getElementById('timeFilter');
    if (timeFilterElement) {
        timeFilterElement.addEventListener('change', handleTimeFilter);
    }
});

function populateOptions(selectElement, start, end) {
    for (let i = start; i <= end; i++) {
        const option = document.createElement('option');
        option.value = i;
        option.text = i;
        selectElement.add(option);
    }
}

function setDefaultSelectedOption(selectElement, defaultValue) {
    for (let i = 0; i < selectElement.options.length; i++) {
        const option = selectElement.options[i];
        if (option.value == defaultValue) {
            option.selected = true;
            break;
        }
    }
}

document.addEventListener('DOMContentLoaded', function () {
    const timeSelect = document.getElementById('timeFilter');
    const monthSelect = document.getElementById('month');
    const weekSelect = document.getElementById('week');
    const statSelect = document.getElementById('statFilter');
    const nCardSelect = document.getElementById('ncardFilter');


    populateOptions(monthSelect, 1, 12);
    populateOptions(weekSelect, 1, 52);

    // Set default selected options
    const defaultFilter = 'all';
    const defaultMonth = 1; // For example, set July as the default month
    const defaultWeek = 1; // For example, set the 3rd week as the default week
    const defaultStat = 's1';
    const defaultNCard = 'n8';

    setDefaultSelectedOption(timeSelect, defaultFilter);
    setDefaultSelectedOption(monthSelect, defaultMonth);
    setDefaultSelectedOption(weekSelect, defaultWeek);
    setDefaultSelectedOption(statSelect, defaultStat);
    setDefaultSelectedOption(nCardSelect, defaultNCard);
});

function displayDecks(decksArray) {
    const container = document.getElementById('deck-container');
    container.innerHTML = ''; // Clear existing decks
    let index = 0;
    const kSelected = document.getElementById('topK').value;


    decksArray.slice(0,kSelected).forEach(deckObj => {
        const deck = new Deck(deckObj.id.replace(/-/g, ''));
        const cards = deck.cards(); // Assuming this method exists

        const deckDiv = document.createElement('div');
        deckDiv.className = 'deck-container';

        const infoDiv = document.createElement('div');
        infoDiv.innerHTML = `<strong>Deck ${++index}</strong><br>
                             Wins: ${deckObj.wins}<br>
                             Ratio: ${deckObj.ratio}<br>
                             Uses: ${deckObj.uses}<br>
                             Number of Players: ${deckObj.nbPlayers}<br>
                             Clan Level: ${deckObj.clanLevel}<br>
                             Average Level: ${deckObj.averageLevel.toFixed(3)}`;
        deckDiv.appendChild(infoDiv);

        cards.forEach(card => {
            const cardDiv = document.createElement('div');
            cardDiv.className = 'card';

            const img = document.createElement('img');
            img.src = card[1]; // Ensure this is the correct property
            img.alt = card[0]; // Ensure this is the correct property

            const name = document.createElement('p');
            name.textContent = card[0]; // Ensure this is the correct property

            cardDiv.appendChild(img);
            cardDiv.appendChild(name);
            deckDiv.appendChild(cardDiv);
        });

        container.appendChild(deckDiv);
    });
}

function validateFilters() {
    const timeFilter = document.getElementById('timeFilter').value;
    const statFilter = document.getElementById('statFilter').value;
    const nCards = document.getElementById('ncardFilter').value;
    let n;
    let s;

    for (let i = 1; i <= 8; i++) {
        if (`n${i}` === nCards)
            n = i;
        console.log(n);
    }

    for (let i = 1; i <= 6; i++) {
        if (`s${i}` === statFilter)
            s = i;  
            console.log(s);
     }


    let granularityType = 'all';
    let granularityNumber = '';

    let additionalFilterValue;
    if (timeFilter === 'month') {
        additionalFilterValue = document.getElementById('month').value;
        granularityType = 'm-';
    } else if (timeFilter === 'week') {
        additionalFilterValue = document.getElementById('week').value;
        granularityType = 'w-';
    } else {
        additionalFilterValue = '';
    }

    granularityNumber = additionalFilterValue;

    const kSelected = document.getElementById('topK').value;

    if(kSelected < 1 || kSelected > maxK) {
        alert(`Please select a number between 1 and ${maxK}`)
    } 
    else {
        alert(`Filters selected - Time: ${timeFilter}, Additional Filter: ${additionalFilterValue}, Stat: ${statFilter}, ncards: ${nCards} , Top K: ${kSelected}`);
        const jsonFilePath = `${dataPath}${granularityType}${granularityNumber}/${statFilter}/data.json`;
        fetchData(jsonFilePath, granularityType + granularityNumber, n, s, kSelected);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const timeFilterElement = document.getElementById('validateButton');
    if (timeFilterElement) {
        timeFilterElement.addEventListener('click', validateFilters);
    }
});

// Function to fetch and parse JSON data
async function fetchData(jsonFilePath, key, ncards, stat, k) {
    try {
        const response = await fetch(jsonFilePath);
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        /*const jsonData = await response.json();
        console.log(jsonData); // This is your JSON object
        displayDecks(jsonData);*/

        await fetch(`http://localhost:3000/api/get/${key}`).then(response => {
            if (!response.ok) {
              throw new Error(`HTTP error! Status: ${response.status}`);
            }
            return response.json();
          }).then(data => {
            console.log(data);
            let d = data.data;
            console.log(d[ncards-1][stat-1]);
            displayDecks(d[ncards-1][stat-1]);
          }).catch(error => {
            console.error('Error:', error);
          });
        // You can now use jsonData as a regular JavaScript object
        // For example, you can access properties like jsonData.propertyName
    } catch (error) {
        console.error('Error fetching data:', error);
    }
}