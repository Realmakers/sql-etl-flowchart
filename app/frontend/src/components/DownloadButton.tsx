import { useCallback } from 'react';
import { useReactFlow, Panel, getRectOfNodes, getTransformForBounds } from 'reactflow';
import { toPng } from 'html-to-image';
import { Download } from 'lucide-react';
import { Button } from '@/components/ui/button';

export default function DownloadButton() {
  const { getNodes } = useReactFlow();

  const onClick = useCallback(() => {
    // we calculate a transform for the nodes so that all nodes are visible
    // we then overwrite the transform of the `.react-flow__viewport` element
    // with the style option of the html-to-image library
    const nodesBounds = getRectOfNodes(getNodes());
    
    if (nodesBounds.width === 0 || nodesBounds.height === 0) return;

    const imageWidth = nodesBounds.width;
    const imageHeight = nodesBounds.height;
    
    // Default transform for the image
    const transform = getTransformForBounds(
      nodesBounds,
      imageWidth,
      imageHeight,
      0.5,
      2
    );

    const viewport = document.querySelector('.react-flow__viewport') as HTMLElement;

    if (viewport) {
      toPng(viewport, {
        backgroundColor: '#fff',
        width: imageWidth,
        height: imageHeight,
        style: {
          width: imageWidth.toString(),
          height: imageHeight.toString(),
          transform: `translate(${transform[0]}px, ${transform[1]}px) scale(${transform[2]})`,
        },
      }).then((dataUrl) => {
        const link = document.createElement('a');
        link.download = 'flowchart.png';
        link.href = dataUrl;
        link.click();
      });
    }
  }, [getNodes]);

  return (
    <Panel position="top-right" className="!mt-2 !mr-2">
      <Button 
        onClick={onClick} 
        variant="outline" 
        size="icon" 
        className="bg-white shadow-lg border border-slate-200 rounded-lg hover:bg-slate-50 w-8 h-8"
        title="下载流程图"
      >
        <Download className="h-4 w-4 text-slate-600" />
      </Button>
    </Panel>
  );
}
