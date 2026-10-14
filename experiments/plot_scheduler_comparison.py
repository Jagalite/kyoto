#!/usr/bin/env python3
"""Render the comparison JSON with matplotlib (optional plotting dependency)."""
import argparse
import json
from pathlib import Path
import statistics


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path)
    parser.add_argument('--output-dir',type=Path,required=True)
    args=parser.parse_args()
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    report=json.loads(args.input.read_text())
    args.output_dir.mkdir(parents=True,exist_ok=True)
    policies=report['policies']
    names=list(report['runs'])
    labels={'fifo-auto':'FIFO auto (1)','fifo-8':'FIFO 8','smart-auto':'Smart auto',
            'smart-no-defer':'Smart, retries off','hill-original':'Hill original','hill-current':'Hill improved'}
    labels.update({p:'Smart fixed '+p.rsplit('-',1)[1] for p in policies if p.startswith('smart-fixed-')})
    metrics=(('useful_work','Retained useful work',False),
             ('normalized_jobs','Job completion progress',False),
             ('restricted_mean_flow','Completion latency, including unfinished jobs',True))
    metric_scores = {}
    for key,title,inverse in metrics:
        values=[]
        for name in names:
            row=[]
            for policy in policies:
                scores=[]
                for outcomes in report['runs'][name].values():
                    candidates=[r['summary'][key] for r in outcomes.values()]
                    own=outcomes[policy]['summary'][key]
                    best=min(candidates) if inverse else max(candidates)
                    score = (best/own if own else 1) if inverse else (own/best if best else 1)
                    scores.append(100*score)
                row.append(statistics.mean(scores))
            values.append(row)
        fig,ax=plt.subplots(figsize=(15,14))
        data=np.array(values)
        im=ax.imshow(data,vmin=0,vmax=100,cmap='viridis',aspect='auto')
        ax.set_xticks(range(len(policies)),[labels[p] for p in policies],rotation=50,ha='right')
        ax.set_yticks(range(len(names)),[n.replace('_',' ') for n in names],fontsize=9)
        for i,row in enumerate(values):
            for j,value in enumerate(row):
                ax.text(j,i,f'{value:.0f}',ha='center',va='center',fontsize=8,color='black' if value>65 else 'white')
        ax.set_title(title+'\nScore relative to best tested policy per seed; higher is better',loc='left',fontsize=15,pad=18)
        fig.colorbar(im,ax=ax,pad=.025,fraction=.025,label='Mean relative score (%)')
        fig.text(.02,.02,f"Synthetic model • {len(report['seeds'])} seeds • {report['seconds']} seconds/run • {report['step']}s integration step\n"
                 'Equal weight per listed scenario; this workload selection is not a production traffic distribution.',fontsize=10)
        fig.subplots_adjust(left=.30,right=.95,bottom=.16,top=.93)
        fig.savefig(args.output_dir/(key+'.png'),dpi=150)
        plt.close(fig)
        metric_scores[key] = {policy: statistics.mean(row[i] for row in values)
                              for i,policy in enumerate(policies)}
    ranked=sorted(report['leaderboard'],key=lambda p:report['leaderboard'][p]['mean_useful_percent_of_best'],reverse=True)
    (args.output_dir/'leaderboard.json').write_text(json.dumps({p:report['leaderboard'][p] for p in ranked},indent=2)+'\n')
    (args.output_dir/'scores.json').write_text(json.dumps(metric_scores,indent=2)+'\n')


if __name__=='__main__':
    main()
