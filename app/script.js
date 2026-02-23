// Sentinela Fluvial - Logic Layer

// Semantic Mappings (REQ 4.1)
const alertConfig = {
    "Alto": { color: "bg-red-100 text-red-700 border-red-200", icon: "fa-exclamation-triangle" },
    "Médio": { color: "bg-orange-100 text-orange-700 border-orange-200", icon: "fa-info-circle" },
    "Baixo": { color: "bg-emerald-100 text-emerald-700 border-emerald-200", icon: "fa-check-circle" }
};

const strategyIcons = {
    "Itinerância": "🛥️",
    "Tratamento Local": "🏥",
    "Remoção": "🚑",
    "Padrão": "📍"
};

// Initialize Application
document.addEventListener("DOMContentLoaded", () => {
    renderSidebar();
});

function renderSidebar() {
    const sidebarList = document.getElementById("sidebar-list");
    sidebarList.innerHTML = "";

    resultadosMedGemma.forEach(item => {
        const card = document.createElement("div");
        card.className = "alert-card bg-white border border-slate-200 p-4 rounded-lg cursor-pointer hover:shadow-md transition-all group";
        card.onclick = () => showDetail(item.id, card);

        const alertStyle = alertConfig[item.response.nivel_alerta] || alertConfig["Baixo"];

        card.innerHTML = `
            <div class="flex items-start gap-3">
                <div class="mt-1">
                    <i class="fas ${alertStyle.icon} ${alertStyle.text}"></i>
                </div>
                <div class="flex-1">
                    <div class="flex justify-between items-center">
                        <h4 class="font-bold text-slate-800 group-hover:text-blue-700 transition-colors">${item.meta.municipality}</h4>
                        <span class="text-[10px] font-bold px-1.5 py-0.5 rounded bg-slate-100 text-slate-500">${item.meta.season}</span>
                    </div>
                    <p class="text-sm text-slate-500">${formatCompetence(item.meta.competence)}</p>
                </div>
            </div>
        `;
        sidebarList.appendChild(card);
    });
}

function showDetail(id, element) {
    const item = resultadosMedGemma.find(r => r.id === id);
    if (!item) return;

    // UI Feedback: Highlight active card
    document.querySelectorAll(".alert-card").forEach(c => c.classList.remove("border-blue-500", "bg-blue-50"));
    element.classList.add("border-blue-500", "bg-blue-50");

    // Hide empty state, show content
    document.getElementById("empty-state").classList.add("hidden");
    document.getElementById("content-display").classList.remove("hidden");

    // Update Meta Information
    document.getElementById("detail-municipality").innerText = item.meta.municipality;
    document.getElementById("detail-meta").innerText = `${formatCompetence(item.meta.competence)} • Estação: ${item.meta.season} • Unidade: ${item.meta.is_fluvial ? 'UBS Fluvial' : 'Unidade Terrestre'}`;

    // Update Tags (REQ 4.1)
    const alertTag = document.getElementById("tag-alert");
    const alertStyle = alertConfig[item.response.nivel_alerta] || alertConfig["Baixo"];
    alertTag.className = `px-3 py-1 rounded-full text-sm font-bold uppercase border ${alertStyle.color}`;
    alertTag.innerText = `Alerta: ${item.response.nivel_alerta}`;

    const strategyTag = document.getElementById("tag-strategy");
    const strategyIcon = strategyIcons[item.response.estrategia_logistica] || strategyIcons["Padrão"];
    strategyTag.innerHTML = `<span>${strategyIcon}</span> ${item.response.estrategia_logistica}`;

    // Update Situational Analysis (REQ 4.2)
    document.getElementById("detail-analysis").innerText = item.response.analise_situacional;

    // Update Recommendations (REQ 4.3)
    const recsList = document.getElementById("detail-recommendations");
    recsList.innerHTML = "";
    item.response.recomendacoes_prioritarias.forEach(rec => {
        const li = document.createElement("li");
        li.className = "flex items-start gap-3 p-3 bg-slate-50 rounded-lg border border-slate-100";
        li.innerHTML = `
            <div class="mt-1 text-emerald-600">
                <i class="far fa-check-square"></i>
            </div>
            <span class="text-slate-700">${rec}</span>
        `;
        recsList.appendChild(li);
    });

    // Reset scroll to top
    document.getElementById("detail-panel").scrollTop = 0;
}

function formatCompetence(comp) {
    const [year, month] = comp.split("-");
    const months = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];
    return `${months[parseInt(month) - 1]} / ${year}`;
}
