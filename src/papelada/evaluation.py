import json
from typing import List, Dict, Any
from pathlib import Path

def _normalize_text(text: Any) -> str:
    """
    Normaliza o texto para comparação:
    - Converte para string (para lidar com null, None, números)
    - Remove espaços em branco das extremidades
    - Converte para minúsculas (requisito do desafio)
    """
    return str(text).strip().lower()

def evaluate_accuracy(predictions: List[Dict[str, Any]], ground_truth: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Compara as predições com o ground truth, calcula a acurácia
    e agrega todas as métricas de performance.
    """
    
    # 1. Criar um mapa de lookup para o ground truth
    gt_map = {Path(item['pdf_path']).name: item['extracted_data'] for item in ground_truth}

    total_fields = 0
    correct_fields = 0
    
    # MUDANÇA: Atualiza as métricas agregadas
    total_metrics = {
        "llm_data_calls": 0,
        "llm_data_tokens": 0,
        "llm_data_time_s": 0.0,
        "llm_regex_calls": 0,
        "llm_regex_tokens": 0,
        "llm_regex_time_s": 0.0,
        "total_llm_calls": 0,
        "total_llm_tokens": 0,
        "total_llm_time_s": 0.0,
        "total_sync_data_time_s": 0.0,      # (Agregado Req 1)
        "total_async_rule_time_s": 0.0,   # (Agregado Req 2)
        "total_combined_time_s": 0.0,     # (Agregado Req 3)
    }
    
    detailed_comparison = []

    # 2. Iterar sobre as predições
    for pred in predictions:
        pdf_key = Path(pred.get('pdf_path_original', pred['pdf_path'])).name
        
        if pdf_key not in gt_map:
            print(f"Aviso: {pdf_key} está nas predições mas não no ficheiro de ground truth. A ignorar.")
            continue
            
        gt_data = gt_map[pdf_key]
        pred_data = pred['extracted_data']
        
        comparison_fields = {}
        
        # 3. Comparar campo a campo
        for field, gt_value in gt_data.items():
            total_fields += 1
            pred_value = pred_data.get(field)
            
            norm_gt_value = _normalize_text(gt_value)
            norm_pred_value = _normalize_text(pred_value)
            
            is_correct = (norm_gt_value == norm_pred_value)
            
            if is_correct:
                correct_fields += 1
                
            comparison_fields[field] = {
                "predicted": pred_value,
                "expected": gt_value,
                "correct": is_correct
            }
            
        # MUDANÇA: Recolhe métricas de performance individuais
        metrics = pred.get('metrics', {})
        
        # (Req 1) Tempo de Resposta (Síncrono) - Pega do orquestrador (nível superior)
        # se não existir, usa o do extractor (aninhado)
        sync_time = pred.get("sync_data_time_s", metrics.get("sync_data_extraction_time_s", 0))
        # (Req 2) Tempo de Aprendizado (Assíncrono)
        async_time = metrics.get("async_rule_generation_time_s", 0)
        # (Req 3) Tempo Total - Pega o cálculo final do extractor
        combined_time = metrics.get("total_processing_time_s", sync_time + async_time) 
        
        # (Req 4) Anexa métricas individuais ao ficheiro
        detailed_comparison.append({
            "pdf_path": pdf_key,
            "comparison": comparison_fields,
            "performance_metrics": {
                "sync_data_time_s": round(sync_time, 3),
                "async_rule_time_s": round(async_time, 3),
                "total_processing_time_s": round(combined_time, 3)
            }
        })
        
        # 4. Agregar Métricas de Custo e Tempo
        for key in total_metrics.keys():
            if key.startswith("total_"): continue # Ignora os nossos novos totais por agora
            if key in metrics:
                total_metrics[key] += metrics.get(key, 0)
        
        # Acumula os novos totais
        total_metrics["total_sync_data_time_s"] += sync_time
        total_metrics["total_async_rule_time_s"] += async_time
        total_metrics["total_combined_time_s"] += combined_time


    # 5. Calcular Totais e Acurácia
    overall_accuracy = (correct_fields / total_fields) if total_fields > 0 else 0
    
    total_metrics["total_llm_calls"] = total_metrics["llm_data_calls"] + total_metrics["llm_regex_calls"]
    total_metrics["total_llm_tokens"] = total_metrics["llm_data_tokens"] + total_metrics["llm_regex_tokens"]
    total_metrics["total_llm_time_s"] = total_metrics["llm_data_time_s"] + total_metrics["llm_regex_time_s"]
    
    # 6. Montar o Relatório Final
    report = {
        "accuracy_summary": {
            "overall_accuracy": f"{overall_accuracy:.2%}",
            "correct_fields": correct_fields,
            "total_fields": total_fields,
        },
        # MUDANÇA: Atualiza o sumário de performance
        "cost_and_performance_summary": {
            "total_documents": len(predictions),
            "avg_sync_data_time_s": round(total_metrics["total_sync_data_time_s"] / len(predictions), 3) if predictions else 0,
            "avg_async_rule_time_s": round(total_metrics["total_async_rule_time_s"] / len(predictions), 3) if predictions else 0,
            "total_sync_data_time_s": round(total_metrics["total_sync_data_time_s"], 3),
            "total_async_rule_time_s": round(total_metrics["total_async_rule_time_s"], 3),
            "total_combined_time_s": round(total_metrics["total_combined_time_s"], 3),
            "total_llm_calls": total_metrics["total_llm_calls"],
            "total_llm_tokens": total_metrics["total_llm_tokens"],
            "total_llm_time_s": round(total_metrics["total_llm_time_s"], 3),
        },
        "metrics_breakdown": total_metrics,
        "detailed_comparison": detailed_comparison # (Req 4) Agora contém métricas por ficheiro
    }
    
    return report